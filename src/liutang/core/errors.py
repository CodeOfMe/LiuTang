class LiuTangError(Exception):
    pass


class PipelineError(LiuTangError):
    pass


class SchemaError(LiuTangError):
    pass


class ConnectorError(LiuTangError):
    pass


class WatermarkError(LiuTangError):
    pass


class StateError(LiuTangError):
    pass