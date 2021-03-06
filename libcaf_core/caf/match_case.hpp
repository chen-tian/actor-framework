/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2015                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_MATCH_CASE_HPP
#define CAF_MATCH_CASE_HPP

#include "caf/none.hpp"
#include "caf/optional.hpp"

#include "caf/skip_message.hpp"
#include "caf/match_case.hpp"

#include "caf/detail/int_list.hpp"
#include "caf/detail/try_match.hpp"
#include "caf/detail/type_list.hpp"
#include "caf/detail/tuple_zip.hpp"
#include "caf/detail/apply_args.hpp"
#include "caf/detail/type_traits.hpp"
#include "caf/detail/pseudo_tuple.hpp"
#include "caf/detail/left_or_right.hpp"
#include "caf/detail/optional_message_visitor.hpp"

namespace caf {

class match_case {
 public:
  enum result {
    fall_through,
    no_match,
    match
  };

  match_case(bool has_wildcard, uint32_t token);

  match_case(match_case&&) = default;
  match_case(const match_case&) = default;

  virtual ~match_case();

  virtual result invoke(optional<message>&, message&) = 0;

  inline uint32_t type_token() const {
    return m_token;
  }

  inline bool has_wildcard() const {
    return m_has_wildcard;
  }

 private:
  bool m_has_wildcard;
  uint32_t m_token;
};

struct match_case_zipper {
  template <class F, typename T>
  auto operator()(const F& fun, T& arg) -> decltype(fun(arg)) const {
    return fun(arg);
  }
  // forward everything as reference if no guard/transformation is set
  template <class T>
  auto operator()(const unit_t&, T& arg) const -> decltype(std::ref(arg)) {
    return std::ref(arg);
  }
};

template <class T>
T& unopt(T& v) {
  return v;
}

template <class T>
T& unopt(optional<T>& v) {
  return *v;
}

struct has_none {
  inline bool operator()() const {
    return false;
  }

  template <class T, class... Ts>
  bool operator()(const T&, const Ts&... xs) const {
    return (*this)(xs...);
  }

  template <class T, class... Ts>
  bool operator()(const optional<T>& x, const Ts&... xs) const {
    return !x || (*this)(xs...);
  }
};

template <bool IsVoid, class F>
class lfinvoker {
 public:
  lfinvoker(F& fun) : m_fun(fun) {
    // nop
  }

  template <class... Ts>
  typename detail::get_callable_trait<F>::result_type operator()(Ts&&... xs) {
    return m_fun(unopt(std::forward<Ts>(xs))...);
  }

 private:
  F& m_fun;
};

template <class F>
class lfinvoker<true, F> {
 public:
  lfinvoker(F& fun) : m_fun(fun) {
    // nop
  }

  template <class... Ts>
  unit_t operator()(Ts&&... xs) {
    m_fun(unopt(std::forward<Ts>(xs))...);
    return unit;
  }

 private:
  F& m_fun;
};

template <class T>
struct projection_result {
  using type = typename detail::get_callable_trait<T>::result_type;
};

template <>
struct projection_result<unit_t> {
  using type = unit_t;
};

template <class F>
class trivial_match_case : public match_case {
 public:
  using fun_trait = typename detail::get_callable_trait<F>::type;

  using plain_result_type = typename fun_trait::result_type;

  using result_type =
    typename std::conditional<
      std::is_reference<plain_result_type>::value,
      plain_result_type,
      typename std::remove_const<plain_result_type>::type
    >::type;

  using arg_types = typename fun_trait::arg_types;

  static constexpr bool is_manipulator =
    detail::tl_exists<
      arg_types,
      detail::is_mutable_ref
    >::value;

  using pattern =
    typename detail::tl_map<
      arg_types,
      std::decay
    >::type;

  using intermediate_tuple =
    typename detail::tl_apply<
      pattern,
      detail::pseudo_tuple
    >::type;

  trivial_match_case(F f)
      : match_case(false, detail::make_type_token_from_list<pattern>()),
        m_fun(std::move(f)) {
    // nop
  }

  match_case::result invoke(optional<message>& res, message& msg) override {
    intermediate_tuple it;
    detail::meta_elements<pattern> ms;
    // check if try_match() reports success
    if (!detail::try_match(msg, ms.arr.data(), ms.arr.size(), it.data)) {
      return match_case::no_match;
    }
    // detach msg before invoking m_fun if needed
    if (is_manipulator) {
      msg.force_detach();
      // update pointers in our intermediate tuple
      for (size_t i = 0; i < msg.size(); ++i) {
        // msg is guaranteed to be detached, hence we don't need to
        // check this condition over and over again via mutable_at
        it[i] = const_cast<void*>(msg.at(i));
      }
    }
    lfinvoker<std::is_same<result_type, void>::value, F> fun{m_fun};
    detail::optional_message_visitor omv;
    auto funres = apply_args(fun, detail::get_indices(it), it);
    res = omv(funres);
    return match_case::match;
  }

 protected:
  F m_fun;
};

template <class F>
class catch_all_match_case : public match_case {
 public:
  using plain_result_type = typename detail::get_callable_trait<F>::result_type;

  using result_type =
    typename std::conditional<
      std::is_reference<plain_result_type>::value,
      plain_result_type,
      typename std::remove_const<plain_result_type>::type
    >::type;

  using arg_types = detail::type_list<>;

  catch_all_match_case(F f)
      : match_case(true, detail::make_type_token<>()),
        m_fun(std::move(f)) {
    // nop
  }

  match_case::result invoke(optional<message>& res, message&) override {
    lfinvoker<std::is_same<result_type, void>::value, F> fun{m_fun};
    auto fun_res = fun();
    detail::optional_message_visitor omv;
    res = omv(fun_res);
    return match_case::match;
  }

 protected:
  F m_fun;
};

template <class F, class Tuple>
class advanced_match_case : public match_case {
 public:
  using tuple_type = Tuple;

  using base_type = advanced_match_case;

  using result_type = typename detail::get_callable_trait<F>::result_type;

  advanced_match_case(bool hw, uint32_t tt, F f)
      : match_case(hw, tt),
        m_fun(std::move(f)) {
    // nop
  }

  virtual bool prepare_invoke(message& msg, tuple_type*) = 0;

  // this function could as well be implemented in `match_case_impl`;
  // however, dealing with all the template parameters in a debugger
  // is just dreadful; this "hack" essentially hides all the ugly
  // template boilterplate types when debugging CAF applications
  match_case::result invoke(optional<message>& res, message& msg) override {
    struct storage {
      storage() : valid(false) {
        // nop
      }
      ~storage() {
        if (valid) {
          data.~tuple_type();
        }
      }
      union { tuple_type data; };
      bool valid;
    };
    storage st;
    if (prepare_invoke(msg, &st.data)) {
      st.valid = true;
      lfinvoker<std::is_same<result_type, void>::value, F> fun{m_fun};
      detail::optional_message_visitor omv;
      auto funres = apply_args(fun, detail::get_indices(st.data), st.data);
      res = omv(funres);
      return match_case::match;
    }
    return match_case::no_match;
  }

 protected:
  F m_fun;
};

template <class Pattern>
struct pattern_has_wildcard {
  static constexpr bool value =
    detail::tl_find<
      Pattern,
      anything
    >::value != -1;
};

template <class Projections>
struct projection_is_trivial {
  static constexpr bool value =
    detail::tl_count_not<
      Projections,
      detail::tbind<std::is_same, unit_t>::template type
    >::value == 0;
};

/*
 * A lifted functor consists of a set of projections, a plain-old
 * functor and its signature. Note that the signature of the lifted
 * functor might differ from the underlying functor, because
 * of the projections.
 */
template <class F, class Pattern, class Projections, class Signature>
class advanced_match_case_impl : public
    advanced_match_case<
      F,
      typename detail::tl_apply<
        typename detail::tl_zip_right<
          typename detail::tl_map<
            Projections,
            projection_result
          >::type,
          typename detail::get_callable_trait<F>::arg_types,
          detail::left_or_right,
          detail::get_callable_trait<F>::num_args
        >::type,
        std::tuple
      >::type> {
 public:
  using plain_result_type = typename detail::get_callable_trait<F>::result_type;

  using result_type =
    typename std::conditional<
      std::is_reference<plain_result_type>::value,
      plain_result_type,
      typename std::remove_const<plain_result_type>::type
    >::type;

  using pattern = Pattern;

  using filtered_pattern =
    typename detail::tl_filter_not_type<
      Pattern,
      anything
    >::type;

  using intermediate_tuple =
    typename detail::tl_apply<
      filtered_pattern,
      detail::pseudo_tuple
    >::type;

  static constexpr uint32_t static_type_token =
    detail::make_type_token_from_list<pattern>();

  // Let F be "R (Ts...)" then match_case<F...> returns optional<R>
  // unless R is void in which case bool is returned
  using optional_result_type =
    typename std::conditional<
      std::is_same<result_type, void>::value,
      optional<unit_t>,
      optional<result_type>
    >::type;

  using projections_list = Projections;

  using projections =
    typename detail::tl_apply<
      projections_list,
      std::tuple
    >::type;

  /*
   * Needed for static type checking when assigning to a typed behavior.
   */
  using arg_types = Signature;

  using fun_args = typename detail::get_callable_trait<F>::arg_types;

  static constexpr size_t num_fun_args = detail::tl_size<fun_args>::value;

  static constexpr bool is_manipulator =
    detail::tl_exists<
      fun_args,
      detail::is_mutable_ref
    >::value;

  using tuple_type =
    typename detail::tl_apply<
      typename detail::tl_zip_right<
        typename detail::tl_map<
          projections_list,
          projection_result
        >::type,
        fun_args,
        detail::left_or_right,
        num_fun_args
      >::type,
      std::tuple
    >::type;

  using super = advanced_match_case<F, tuple_type>;

  advanced_match_case_impl() = default;

  advanced_match_case_impl(const advanced_match_case_impl&) = default;

  advanced_match_case_impl& operator=(advanced_match_case_impl&&) = default;

  advanced_match_case_impl& operator=(const advanced_match_case_impl&) = default;

  advanced_match_case_impl(F f)
      : super(pattern_has_wildcard<Pattern>::value, static_type_token,
              std::move(f)) {
    // nop
  }

  advanced_match_case_impl(F f, projections ps)
      : super(pattern_has_wildcard<Pattern>::value, static_type_token,
              std::move(f)),
        m_ps(std::move(ps)) {
    // nop
  }

  bool prepare_invoke(message& msg, tuple_type* out) {
    // detach msg before invoking m_fun if needed
    if (is_manipulator) {
      msg.force_detach();
    }
    intermediate_tuple it;
    detail::meta_elements<pattern> ms;
    // check if try_match() reports success
    if (!detail::try_match(msg, ms.arr.data(), ms.arr.size(), it.data)) {
      return false;
    }
    match_case_zipper zip;
    using indices_type = typename detail::il_indices<intermediate_tuple>::type;
    //indices_type indices;
    typename detail::il_take<indices_type, detail::tl_size<projections>::value - num_fun_args>::type lefts;
    typename detail::il_right<indices_type, num_fun_args>::type rights;
    has_none hn;
    // check if guards of discarded arguments are fulfilled
    auto lhs_tup = tuple_zip(zip, lefts, m_ps, it);
    if (detail::apply_args(hn, detail::get_indices(lhs_tup), lhs_tup)) {
      return false;
    }
    // zip remaining arguments into output tuple
    new (out) tuple_type(tuple_zip(zip, rights, m_ps, it));
    //tuple_type rhs_tup = tuple_zip(zip, rights, m_ps, it);
    // check if remaining guards are fulfilled
    if (detail::apply_args(hn, detail::get_indices(*out), *out)) {
      out->~tuple_type();
      return false;
    }
    return true;
  }

 private:
  projections m_ps;
};

struct match_case_info {
  bool has_wildcard;
  uint32_t type_token;
  match_case* ptr;
};

inline bool operator<(const match_case_info& x, const match_case_info& y) {
  return x.type_token < y.type_token;
}

template <class F>
typename std::enable_if<
  !std::is_base_of<match_case, F>::value,
  std::tuple<trivial_match_case<F>>
>::type
to_match_case_tuple(F fun) {
  return std::make_tuple(std::move(fun));
}

template <class MatchCase>
typename std::enable_if<
  std::is_base_of<match_case, MatchCase>::value,
  std::tuple<MatchCase&>
>::type
to_match_case_tuple(MatchCase& x) {
  return std::tie(x);
}

template <class... Ts>
std::tuple<Ts...>& to_match_case_tuple(std::tuple<Ts...>& x) {
  static_assert(detail::conjunction<
                  std::is_base_of<
                    match_case,
                    Ts
                  >::value...
                >::value,
                "to_match_case_tuple received a tuple of non-match_case Ts");
  return x;
}

template <class T, class U>
typename std::enable_if<
  std::is_base_of<match_case, T>::value || std::is_base_of<match_case, U>::value
>::type
operator,(T, U) {
  static_assert(!std::is_same<T, T>::value,
                "this syntax is not supported -> you probably did "
                "something like 'return (...)' instead of 'return {...}'");
}

} // namespace caf

#endif // CAF_MATCH_CASE_HPP
