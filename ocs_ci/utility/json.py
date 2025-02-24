import json


class SetToListJSONEncoder(json.JSONEncoder):
    """
    The CustomJSONEncoder class is a subclass of json.JSONEncoder designed to handle the serialization of Python
    objects into JSON format, with a specific focus on converting set objects into lists.
    This is necessary because the default JSON encoder in Python does not support set objects, which are not a valid
    JSON data type.
    This way we avoid "TypeError: Object of type set is not JSON serializable"

    Usage:
        json.dumps(data, cls=SetToListJSONEncoder)
    """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)
