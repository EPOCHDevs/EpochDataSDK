#pragma once

#include <drogon/drogon.h>
#include <trantor/net/EventLoopThread.h>
#include <spdlog/spdlog.h>
#include <memory>

namespace data_sdk::common {

/**
 * Helper class to manage event loop selection between Drogon's main loop
 * and a dedicated event loop thread.
 */
class EventLoopHelper {
public:
  /**
   * Get an event loop based on configuration.
   *
   * @param use_drogon_loop If true, attempts to use Drogon's main loop
   * @param component_name Name of the component (for logging)
   * @param loop_thread Output parameter for the created thread (if any)
   * @return Pointer to the selected event loop
   */
  static trantor::EventLoop* getEventLoop(
      bool use_drogon_loop,
      const std::string& component_name,
      std::unique_ptr<trantor::EventLoopThread>& loop_thread) {

    trantor::EventLoop* loop = nullptr;

    if (use_drogon_loop) {
      // Try to use Drogon's main loop if available
      try {
        auto& app = drogon::app();
        if (app.isRunning()) {
          loop = app.getLoop();
          SPDLOG_INFO("{}: Using Drogon's main event loop", component_name);
        } else {
          SPDLOG_WARN("{}: Drogon app not running, creating own event loop",
                     component_name);
        }
      } catch (const std::exception& e) {
        SPDLOG_WARN("{}: Failed to get Drogon app ({}), creating own event loop",
                   component_name, e.what());
      } catch (...) {
        SPDLOG_WARN("{}: Failed to get Drogon app, creating own event loop",
                   component_name);
      }
    }

    if (!loop) {
      // Create our own event loop thread
      loop_thread = std::make_unique<trantor::EventLoopThread>();
      loop_thread->run();
      loop = loop_thread->getLoop();
      SPDLOG_DEBUG("{}: Created dedicated event loop thread", component_name);
    }

    return loop;
  }

  /**
   * Safely quit an event loop thread if it exists.
   *
   * @param loop_thread The event loop thread to quit
   */
  static void quitEventLoopThread(
      std::unique_ptr<trantor::EventLoopThread>& loop_thread) {
    if (loop_thread && loop_thread->getLoop()) {
      loop_thread->getLoop()->quit();
    }
  }
};

} // namespace data_sdk::common