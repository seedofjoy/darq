class DummyDbEngine:
    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass


async def create_engine() -> DummyDbEngine:
    return DummyDbEngine()
