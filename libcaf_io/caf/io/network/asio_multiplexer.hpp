/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2015                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 * Raphael Hiesgen <raphael.hiesgen (at) haw-hamburg.de>                      *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_IO_NETWORK_ASIO_MULTIPLEXER_HPP
#define CAF_IO_NETWORK_ASIO_MULTIPLEXER_HPP

#include "boost/asio.hpp"

#include "caf/io/receive_policy.hpp"

#include "caf/io/network/multiplexer.hpp"
#include "caf/io/network/stream_manager.hpp"
#include "caf/io/network/acceptor_manager.hpp"

#include "caf/detail/logging.hpp"

namespace caf {
namespace io {
namespace network {

/**
 * @brief Low-level backend for IO multiplexing.
 */
using io_backend = boost::asio::io_service;

/**
 * @brief Makes sure a {@link multiplexer} does not stop its event loop
 *        before the application requests a shutdown.
 *
 * The supervisor informs the multiplexer in its constructor that it
 * must not exit the event loop until the destructor of the supervisor
 * has been called.
 */
using supervisor = boost::asio::io_service::work;

/**
 * @brief Low-level socket type used as default.
 */
using default_socket = boost::asio::ip::tcp::socket;

/**
 * @brief Low-level socket type used as default.
 */
using default_socket_acceptor = boost::asio::ip::tcp::acceptor;

/**
 * @brief Platform-specific native socket type.
 */
using native_socket = typename default_socket::native_handle_type;

/**
 * @brief Platform-specific native acceptor socket type.
 */
using native_socket_acceptor = typename default_socket_acceptor::native_handle_type;

class asio_multiplexer : public multiplexer {
 public:
  friend class io::middleman;
  friend class supervisor;

  connection_handle new_tcp_scribe(const std::string&, uint16_t) override;

  void assign_tcp_scribe(broker*, connection_handle hdl) override;

  template<class Socket>
  connection_handle add_tcp_scribe(broker*, Socket&& sock);

  connection_handle add_tcp_scribe(broker*, native_socket fd) override;

  connection_handle add_tcp_scribe(broker*, const std::string& host,
                                   uint16_t port) override;

  std::pair<accept_handle, uint16_t>
  new_tcp_doorman(uint16_t p, const char* in, bool rflag) override;

  void assign_tcp_doorman(broker*, accept_handle hdl) override;

  accept_handle add_tcp_doorman(broker*, default_socket_acceptor&& sock);

  accept_handle add_tcp_doorman(broker*, native_socket fd) override;

  std::pair<accept_handle, uint16_t>
  add_tcp_doorman(broker*, uint16_t port, const char* in, bool rflag) override;

  void dispatch_runnable(runnable_ptr ptr) override;

  asio_multiplexer();

  ~asio_multiplexer();

  supervisor_ptr make_supervisor() override;

  void run() override;

 private:

  inline boost::asio::io_service& backend() {
    return m_backend;
  }

  io_backend m_backend;
  std::mutex m_mtx_sockets;
  std::mutex m_mtx_acceptors;
  std::map<int64_t,default_socket> m_unassigned_sockets;
  std::map<int64_t,default_socket_acceptor> m_unassigned_acceptors;
};

asio_multiplexer& get_multiplexer_singleton();

template <class T>
connection_handle conn_hdl_from_socket(T& sock) {
  return connection_handle::from_int(
    int64_from_native_socket(sock.native_handle()));
}

template <class T>
accept_handle accept_hdl_from_socket(T& sock) {
  return accept_handle::from_int(
        int64_from_native_socket(sock.native_handle()));
}

// not sure about this

/**
 * @relates manager
 */
using manager_ptr = intrusive_ptr<manager>;

/**
 * @brief A stream capable of both reading and writing. The stream's input
 *        data is forwarded to its {@link stream_manager manager}.
 */
template<class Socket>
class stream {

 public:

    /**
     * @brief A smart pointer to a stream manager.
     */
    using manager_ptr = intrusive_ptr<stream_manager>;

    /**
     * @brief A buffer class providing a compatible
     *        interface to @p std::vector.
     */
    using buffer_type = std::vector<char>;

    stream(io_backend& backend) : m_writing(false), m_fd(backend) {
        configure_read(receive_policy::at_most(1024));
    }

    /**
     * @brief Returns the @p multiplexer this stream belongs to.
     */
    inline io_backend& backend() {
        return m_fd.get_io_service();
    }

    /**
     * @brief Returns the IO socket.
     */
    inline Socket& socket_handle() {
        return m_fd;
    }

    /**
     * @brief Initializes this stream, setting the socket handle to @p fd.
     */
    void init(Socket fd) {
        m_fd = std::move(fd);
    }

    /**
     * @brief Starts reading data from the socket, forwarding incoming
     *        data to @p mgr.
     */
    void start(const manager_ptr& mgr) {
        // BOOST_ACTOR_REQUIRE(mgr != nullptr);
        read_loop(mgr);
    }

    /**
     * @brief Configures how much data will be provided
     *        for the next @p consume callback.
     * @warning Must not be called outside the IO multiplexers event loop
     *          once the stream has been started.
     */
    void configure_read(receive_policy::config config) {
        m_rd_flag = config.first;
        m_rd_size = config.second;
    }

    /**
     * @brief Copies data to the write buffer.
     * @note Not thread safe.
     */
    void write(const void* buf, size_t num_bytes) {
        auto first = reinterpret_cast<const char*>(buf);
        auto last  = first + num_bytes;
        m_wr_offline_buf.insert(first, last);
    }

    /**
     * @brief Returns the write buffer of this stream.
     * @warning Must not be modified outside the IO multiplexers event loop
     *          once the stream has been started.
     */
    buffer_type& wr_buf() {
        return m_wr_offline_buf;
    }

    buffer_type& rd_buf() {
        return m_rd_buf;
    }

    /**
     * @brief Sends the content of the write buffer, calling
     *        the @p io_failure member function of @p mgr in
     *        case of an error.
     * @warning Must not be called outside the IO multiplexers event loop
     *          once the stream has been started.
     */
    void flush(const manager_ptr& mgr) {
        // BOOST_ACTOR_REQUIRE(mgr != nullptr);
        if (!m_wr_offline_buf.empty() && !m_writing) {
            m_writing = true;
            write_loop(mgr);
        }
    }

    /**
     * @brief Closes the network connection, thus stopping this stream.
     */
    void stop() {
        CAF_LOGMF(CAF_TRACE, "");
        m_fd.close();
    }

    void stop_reading() {
        CAF_LOGMF(CAF_TRACE, "");
        boost::system::error_code ec; // ignored
        m_fd.shutdown(boost::asio::ip::tcp::socket::shutdown_receive, ec);
    }

 private:

    void write_loop(const manager_ptr& mgr) {
        if (m_wr_offline_buf.empty()) {
            m_writing = false;
            return;
        }
        m_wr_buf.clear();
        m_wr_buf.swap(m_wr_offline_buf);
        boost::asio::async_write(m_fd, boost::asio::buffer(m_wr_buf),
                          [=](const boost::system::error_code& ec, size_t nb) {
            CAF_LOGC(CAF_TRACE, "caf::io::network::stream",
                                "write_loop$lambda",
                                BOOST_ACTOR_ARG(this));
            static_cast<void>(nb); // silence compiler warning
            if (!ec) {
                CAF_LOGC(CAF_DEBUG, "caf::io::network::stream",
                                    "write_loop$lambda",
                                    nb << " bytes sent");
                write_loop(mgr);
            }
            else {
                CAF_LOGC(CAF_DEBUG, "caf::io::network::stream",
                                    "write_loop$lambda",
                                    "error during send: " << ec.message());
                mgr->io_failure(operation::read);
                m_writing = false;
            }
        });
    }

    void read_loop(const manager_ptr& mgr) {
        auto cb = [=](const boost::system::error_code& ec, size_t read_bytes) {
            CAF_LOGC(CAF_TRACE, "caf::io::network::stream",
                                "read_loop$cb",
                                CAF_ARG(this));
            if (!ec) {
                mgr->consume(m_rd_buf.data(), read_bytes);
                read_loop(mgr);
            }
            else mgr->io_failure(operation::read);
        };
        switch (m_rd_flag) {
            case receive_policy_flag::exactly:
                if (m_rd_buf.size() < m_rd_size) m_rd_buf.resize(m_rd_size);
                boost::asio::async_read(m_fd, boost::asio::buffer(m_rd_buf, m_rd_size), cb);
                break;
            case receive_policy_flag::at_most:
                if (m_rd_buf.size() < m_rd_size) m_rd_buf.resize(m_rd_size);
                m_fd.async_read_some(boost::asio::buffer(m_rd_buf, m_rd_size), cb);
                break;
            case receive_policy_flag::at_least: {
                // read up to 10% more, but at least allow 100 bytes more
                auto min_size =   m_rd_size
                                + std::max<size_t>(100, m_rd_size / 10);
                if (m_rd_buf.size() < min_size) m_rd_buf.resize(min_size);
                collect_data(mgr, 0);
                break;
            }
        }
    }

    void collect_data(const manager_ptr& mgr, size_t collected_bytes) {
        m_fd.async_read_some(boost::asio::buffer(m_rd_buf.data() + collected_bytes,
                                          m_rd_buf.size() - collected_bytes),
                            [=](const boost::system::error_code& ec, size_t nb) {
            CAF_LOGC(CAF_TRACE, "caf::io::network::stream",
                                "collect_data$lambda",
                                CAF_ARG(this));
            if (!ec) {
                auto sum = collected_bytes + nb;
                if (sum >= m_rd_size) {
                    mgr->consume(m_rd_buf.data(), sum);
                    read_loop(mgr);
                }
                else collect_data(mgr, sum);
            }
            else mgr->io_failure(operation::write);
        });
    }

    bool                m_writing;
    Socket              m_fd;
    receive_policy_flag m_rd_flag;
    size_t              m_rd_size;
    buffer_type         m_rd_buf;
    buffer_type         m_wr_buf;
    buffer_type         m_wr_offline_buf;

};

/**
 * @brief An acceptor is responsible for accepting incoming connections.
 */
template<class SocketAcceptor>
class acceptor {
  using protocol_type = typename SocketAcceptor::protocol_type;
  using socket_type   = boost::asio::basic_stream_socket<protocol_type>;

 public:
  /**
   * @brief A manager providing the @p accept member function.
   */
  using manager_type = acceptor_manager;

  /**
   * @brief A smart pointer to an acceptor manager.
   */
  using manager_ptr = intrusive_ptr<manager_type>;

  acceptor(io_backend& io, asio_multiplexer& backend)
  : m_backend(backend), m_accept_fd(io), m_fd(io) { }

  /**
   * @brief Returns the @p multiplexer this acceptor belongs to.
   */
  inline asio_multiplexer& backend() {
    return m_backend;
  }

  /**
   * @brief Returns the IO socket.
   */
  inline SocketAcceptor& socket_handle() {
    return m_accept_fd;
  }

  /**
   * @brief Returns the accepted socket. This member function should
   *        be called only from the @p new_connection callback.
   */
  inline socket_type& accepted_socket() {
    return m_fd;
  }

  /**
   * @brief Initializes this acceptor, setting the socket handle to @p fd.
   */
  void init(SocketAcceptor fd) {
    m_accept_fd = std::move(fd);
  }

  /**
   * @brief Starts this acceptor, forwarding all incoming connections to
   *        @p manager. The intrusive pointer will be released after the
   *        acceptor has been closed or an IO error occured.
   */
  void start(const manager_ptr& mgr) {
    // BOOST_ACTOR_REQUIRE(mgr != nullptr);
    accept_loop(mgr);
  }

  /**
   * @brief Closes the network connection, thus stopping this acceptor.
   */
  void stop() {
      m_accept_fd.close();
  }

 private:
  void accept_loop(const manager_ptr& mgr) {
    m_accept_fd.async_accept(m_fd, [=](const boost::system::error_code& ec) {
      CAF_LOGMF(CAF_TRACE, "");
      if (!ec) {
        mgr->new_connection(); // probably moves m_fd
        // reset m_fd for next accept operation
        m_fd = socket_type{m_accept_fd.get_io_service()};
        accept_loop(mgr);
      }
      else mgr->io_failure(operation::read);
    });
  }

  asio_multiplexer& m_backend;
  SocketAcceptor    m_accept_fd;
  socket_type       m_fd;
};



} // namesapce network
} // namespace io
} // namespace caf


#endif // CAF_IO_NETWORK_ASIO_MULTIPLEXER_HPP
