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

#include "caf/io/network/asio_multiplexer.hpp"

namespace caf {
namespace io {
namespace network {

  connection_handle asio_multiplexer::new_tcp_scribe(const std::string&,
                                                     uint16_t) {

  }

  void asio_multiplexer::assign_tcp_scribe(broker* ptr, connection_handle hdl) {

  }

//  connection_handle add_tcp_scribe(broker*, default_socket_acceptor&& sock);

  connection_handle asio_multiplexer::add_tcp_scribe(broker*,
                                                     native_socket fd) {

  }

  connection_handle asio_multiplexer::add_tcp_scribe(broker*, const std::string& h,
                                                     uint16_t port) {

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
  asio_multiplexer::add_tcp_doorman(broker*, uint16_t p, const char* in, bool rflag) {

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
