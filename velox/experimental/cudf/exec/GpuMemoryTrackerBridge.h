#pragma once

#include <dlfcn.h>
#include <string>

namespace facebook::velox::cudf_velox {

// Velox is built independently from Gluten, so GPU memory attribution crosses
// that boundary through optional C symbols exported by libgluten.so. Resolve
// through dlsym instead of weak references so the standalone Velox build stays
// independent and the symbols can still bind when the code is linked into
// libgluten.so.
class ScopedGpuMemoryOperatorContext {
 public:
  using CurrentFn = const char* (*)();
  using SetFn = void (*)(const char*);
  using ClearFn = void (*)();

  explicit ScopedGpuMemoryOperatorContext(const std::string& context) {
    auto* fns = functions();
    if (fns->set == nullptr) {
      return;
    }
    if (fns->current != nullptr) {
      if (const char* previous = fns->current()) {
        previousContext_ = previous;
      }
    }
    fns->set(context.c_str());
    active_ = true;
  }

  ~ScopedGpuMemoryOperatorContext() {
    if (!active_) {
      return;
    }
    auto* fns = functions();
    if (fns->set == nullptr) {
      return;
    }
    if (!previousContext_.empty()) {
      fns->set(previousContext_.c_str());
      return;
    }
    if (fns->clear != nullptr) {
      fns->clear();
    } else {
      fns->set("");
    }
  }

  ScopedGpuMemoryOperatorContext(const ScopedGpuMemoryOperatorContext&) =
      delete;
  ScopedGpuMemoryOperatorContext& operator=(
      const ScopedGpuMemoryOperatorContext&) = delete;

 private:
  struct Functions {
    CurrentFn current{nullptr};
    SetFn set{nullptr};
    ClearFn clear{nullptr};
  };

  static Functions* functions() {
    static Functions fns{
        reinterpret_cast<CurrentFn>(dlsym(
            RTLD_DEFAULT, "glutenGpuMemoryTrackerCurrentOperatorContext")),
        reinterpret_cast<SetFn>(dlsym(
            RTLD_DEFAULT, "glutenGpuMemoryTrackerSetCurrentOperatorContext")),
        reinterpret_cast<ClearFn>(dlsym(
            RTLD_DEFAULT, "glutenGpuMemoryTrackerClearCurrentOperatorContext"))};
    return &fns;
  }

  bool active_{false};
  std::string previousContext_;
};

} // namespace facebook::velox::cudf_velox
