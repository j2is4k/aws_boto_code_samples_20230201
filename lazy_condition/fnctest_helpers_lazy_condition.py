import pytest

from common.helpers import LazyCondition as C


class TestLazyCondition:
    def test_matching_rule(self):
        rule = C("status").equals("125")
        assert rule(status="125")

        rule = C("channel").startswith("_test") & C("value").__lt__(10)
        assert rule(channel="_testX", value=5)

    def test_direct_and_indirect_access(self):
        rule = C("status").contains("op")
        assert rule(status="stop")

        rule = C("status").__contains__("op")
        assert rule(status="stop")

    def test_false_values(self):
        rule = C("status").equals("")
        assert rule(status="")

        rule = C("int").__le__(0)
        assert rule(int=0)

        rule = C("int").__le__(0)
        assert rule(int=-1)

        rule = C("status").equals("end")
        assert not rule(status=123)

    def test_non_matching_rule(self):
        rule = C("status").endswith("end")
        assert not rule(status="125start")

        rule = C("channel").startswith("_test") & C("value").__eq__(10)
        assert not rule(channel="_testX", value=5)

    def test_negation(self):
        rule = C("value").equals(55) & C("status").equals("end").invert()
        assert rule(status="125start", value=55)

        rule = C("status").equals("end").invert() & C("value").equals(55)
        assert rule(status="125start", value=55)

        rule = C("value").equals(55) & C("status").equals("end").invert()
        assert not rule(status="end", value=55)

        rule = C("status").equals("end").invert() & C("value").equals(55)
        assert not rule(status="end", value=55)


if __name__ == "__main__":
    pytest.main([__file__])
