class LiuTangError(Exception):
    pass


class EngineNotAvailableError(LiuTangError):
    def __init__(self, engine: str, detail: str = ""):
        self.engine = engine
        msg = f"Engine '{engine}' is not available"
        if detail:
            msg += f": {detail}"
        msg += ". Install with: pip install liutang[{engine}]"
        super().__init__(msg)


class EngineVersionError(LiuTangError):
    def __init__(self, engine: str, required: str, found: str):
        self.engine = engine
        self.required = required
        self.found = found
        super().__init__(
            f"{engine}: required version >={required}, found {found}. "
            f"Run: pip install {engine}>={required}"
        )


class SchemaError(LiuTangError):
    pass


class PipelineError(LiuTangError):
    pass


class ConnectorError(LiuTangError):
    pass


class SerializationError(LiuTangError):
    pass