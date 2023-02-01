class LazyCondition:
    """Lazy condition evaluator supporting only bitwise "and" logical operation ("&", not "and").

    Condition rule is given at construction time, evaluated first on calling the instance time.

    """

    def __init__(self, name):
        self._variable_name = name
        self._operation_name = ""
        self._operand = ""
        self._invert = False
        self._chained__and__conditions = [self]

    def __call__(self, **namespace):
        for condition in self._chained__and__conditions:
            value = namespace.get(condition._variable_name)
            if value is None:
                return False
            operation_fnc = getattr(value, condition._operation_name)
            result = operation_fnc(condition._operand)
            if not isinstance(result, bool) or result == condition._invert:
                return False
        return True

    def __getattribute__(self, name):
        if hasattr(super(), name):
            # if method in base class, assume the caller wants the method on actual condition variable
            raise AttributeError
        return object.__getattribute__(self, name)

    def __getattr__(self, name):
        return lambda operand: self._init_condition(name, operand)

    def __and__(self, condition: "LazyCondition"):
        self._chained__and__conditions.append(condition)
        return self

    def _init_condition(self, operation_name, operand):
        self._operation_name = operation_name
        self._operand = operand
        return self

    def equals(self, operand):
        return self._init_condition("__eq__", operand)

    def contains(self, operand):
        # you can also use __contains__ directly
        return self._init_condition("__contains__", operand)

    def invert(self):
        self._invert = not self._invert
        return self


