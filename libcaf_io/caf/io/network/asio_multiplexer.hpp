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

#include "caf/io/network/multiplexer.hpp"
#include "caf/io/network/stream_manager.hpp"
#include "caf/io/network/acceptor_manager.hpp"

#include "caf/detail/logging.hpp"

namespace caf {
namespace io {
namespace network {

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

  asio_multiplexer();

  ~asio_multiplexer();

 private:

};

asio_multiplexer& get_multiplexer_singleton();


} // namesapce network
} // namespace io
} // namespace caf


#endif // CAF_IO_NETWORK_ASIO_MULTIPLEXER_HPP
