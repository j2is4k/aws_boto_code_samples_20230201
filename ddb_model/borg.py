def borg_factory():
    class Borg:
        INSTANTIATE_IN_SECONDS = 300
        _shared_state = {}

        def __init__(self, force_reload=False):
            if force_reload or time.time() - self._shared_state.get("_start_time", 0) > self.INSTANTIATE_IN_SECONDS:
                # Keep reference on shared state all the time.
                self._shared_state.clear()
                self._shared_state["_start_time"] = time.time()
            self.__dict__ = self._shared_state
            remaining_time = self.INSTANTIATE_IN_SECONDS - time.time() + self.__dict__["_start_time"]
            logger.debug(f"Shared instance lives further {remaining_time:3.0f} s.")

        def clear(self):
            self._shared_state.clear()

    return Borg
    
    