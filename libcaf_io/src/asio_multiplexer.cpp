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

#include "caf/exception.hpp"

#include "caf/io/broker.hpp"

#include "caf/io/network/asio_multiplexer.hpp"

namespace caf {
namespace io {
namespace network {


default_socket new_tcp_connection(io_backend& backend,
                                  const std::string& host, uint16_t port) {
  default_socket fd{backend};
  using boost::asio::ip::tcp;
  try {
    tcp::resolver r(fd.get_io_service());
    tcp::resolver::query q(host, std::to_string(port));
    auto i = r.resolve(q);
    boost::asio::connect(fd, i);
  }
  catch (boost::system::system_error& se) {
    throw network_error(se.code().message());
  }
  return fd;
}

connection_handle asio_multiplexer::new_tcp_scribe(const std::string& host,
                                                   uint16_t port) {
  default_socket fd{new_tcp_connection(backend(), host, port)};
  auto id = int64_from_native_socket(fd.native_handle());
  std::lock_guard<std::mutex> lock(m_mtx);
  m_unassigned_sockets.insert(std::make_pair(id, std::move(fd)));
  return connection_handle::from_int(id);
}

void asio_multiplexer::assign_tcp_scribe(broker* self, connection_handle hdl) {
  std::lock_guard<std::mutex> lock(m_mtx);
  auto itr = m_unassigned_sockets.find(hdl.id());
  if (itr == m_unassigned_sockets.end()) {
    add_tcp_scribe(self, std::move(itr->second));
    m_unassigned_sockets.erase(itr);
  }
}

connection_handle asio_multiplexer::add_tcp_scribe(broker* self,
                                                   default_socket&& sock) {
  CAF_LOG_TRACE("");
  class impl : public broker::scribe {
   public:
    impl(broker* ptr, default_socket&& s)
      : scribe(ptr, network::conn_hdl_from_socket(s)),
        m_launched(false),
        m_stream(s.get_io_service()) {
      m_stream.init(std::move(s));
    }
    void configure_read(receive_policy::config config) override {
      CAF_LOG_TRACE("");
      m_stream.configure_read(config);
      if (!m_launched) launch();
    }
    broker::buffer_type& wr_buf() override {
      return m_stream.wr_buf();
    }
    broker::buffer_type& rd_buf() override {
      return m_stream.rd_buf();
    }
    void stop_reading() override {
      CAF_LOG_TRACE("");
      m_stream.stop_reading();
      disconnect(false);
    }
    void flush() override {
      CAF_LOG_TRACE("");
      m_stream.flush(this);
    }
    void launch() {
      CAF_LOG_TRACE("");
      CAF_ASSERT(!m_launched);
      m_launched = true;
      m_stream.start(this);
    }
   private:
    bool m_launched;
    stream<default_socket> m_stream;
  };
  broker::scribe_pointer ptr = make_counted<impl>(self, std::move(sock));
  self->add_scribe(ptr);
  return ptr->hdl();
}

connection_handle asio_multiplexer::add_tcp_scribe(broker* self,
                                                   native_socket fd) {
  CAF_LOG_TRACE(CAF_ARG(self) << ", " << CAF_ARG(fd));
  boost::system::error_code error;
  boost::asio::ip::tcp::socket sock(backend());
  auto ec = sock.assign(boost::asio::ip::tcp::v6(), fd, error);
  if (ec || error) {
    ec = sock.assign(boost::asio::ip::tcp::v4(), fd, error);
  }
  if (ec || error) {
    throw network_error(ec.message());
  }
  return add_tcp_scribe(self, std::move(sock));
}

connection_handle asio_multiplexer::add_tcp_scribe(broker* self,
                                                   const std::string& host,
                                                   uint16_t port) {
  CAF_LOG_TRACE(CAF_ARG(self) << ", " << CAF_ARG(host)
                << ", " << CAF_ARG(port));
  return add_tcp_scribe(self, new_tcp_connection(backend(), host, port));
}

std::pair<accept_handle, uint16_t>
asio_multiplexer::new_tcp_doorman(uint16_t p, const char* in, bool rflag) {

}

void asio_multiplexer::assign_tcp_doorman(broker* ptr, accept_handle hdl) {

}

//  accept_handle add_tcp_doorman(broker*, default_socket_acceptor&& sock);

accept_handle asio_multiplexer::add_tcp_doorman(broker*, native_socket fd) {

}

std::pair<accept_handle, uint16_t>
asio_multiplexer::add_tcp_doorman(broker*, uint16_t p,
                                  const char* in, bool rflag) {

}

void asio_multiplexer::dispatch_runnable(runnable_ptr ptr) {

}

asio_multiplexer::asio_multiplexer() {

}

asio_multiplexer::~asio_multiplexer() {

}

multiplexer::supervisor_ptr asio_multiplexer::make_supervisor() {

}

void asio_multiplexer::run() {

}

} // namesapce network
} // namespace io
} // namespace caf
