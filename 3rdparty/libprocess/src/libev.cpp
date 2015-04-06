#include <ev.h>

#include <queue>

#include <stout/duration.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>

#include "event_loop.hpp"
#include "libev.hpp"

namespace process {

// Defines the initial values for all of the declarations made in
// libev.hpp (since these need to live in the static data space).
struct ev_loop* loop = NULL;

ev_async asyncWatcher;

std::queue<ev_io*>* watchers = new std::queue<ev_io*>();

synchronizable(watchers);

std::queue<lambda::function<void(void)>>* functions =
  new std::queue<lambda::function<void(void)>>();

ThreadLocal<bool>* _inEventLoop_ = new ThreadLocal<bool>();


void handleAsync(struct ev_loop* loop, ev_async* _, int revents)
{
  synchronized (watchers) {
    // Start all the new I/O watchers.
    while (!watchers->empty()) {
      ev_io* watcher = watchers->front();
      watchers->pop();
      ev_io_start(loop, watcher);
    }

    while (!functions->empty()) {
      (functions->front())();
      functions->pop();
    }
  }
}


void EventLoop::initialize()
{
  synchronizer(watchers) = SYNCHRONIZED_INITIALIZER;

  loop = ev_default_loop(EVFLAG_AUTO);

  ev_async_init(&asyncWatcher, handleAsync);
  ev_async_start(loop, &asyncWatcher);
}


namespace internal {

void handleDelay(struct ev_loop* loop, ev_timer* timer, int revents)
{
  lambda::function<void(void)>* function =
    reinterpret_cast<lambda::function<void(void)>*>(timer->data);
  (*function)();
  delete function;
  ev_timer_stop(loop, timer);
  delete timer;
}


Future<Nothing> delay(
    const Duration& duration,
    const lambda::function<void(void)>& function)
{
  ev_timer* timer = new ev_timer();
  timer->data = reinterpret_cast<void*>(
      new lambda::function<void(void)>(function));

  // Determine the 'after' parameter to pass to libev and set it to 0
  // in the event that it's negative so that we always make sure to
  // invoke 'function' even if libev doesn't support negative 'after'
  // values.
  double after = duration.secs();

  if (after < 0) {
    after = 0;
  }

  const double repeat = 0.0;

  ev_timer_init(timer, handleDelay, after, repeat);
  ev_timer_start(loop, timer);

  return Nothing();
}

} // namespace internal {


void EventLoop::delay(
    const Duration& duration,
    const lambda::function<void(void)>& function)
{
  runInEventLoop<Nothing>(
      lambda::bind(&internal::delay, duration, function));
}


double EventLoop::time()
{
  // TODO(benh): Versus ev_now()?
  return ev_time();
}


void* EventLoop::run(void*)
{
  __inEventLoop__ = true;

  ev_loop(loop, 0);

  __inEventLoop__ = false;

  return NULL;
}

} // namespace process {
