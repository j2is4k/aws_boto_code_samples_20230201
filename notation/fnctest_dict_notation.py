import pytest

from common.notation import DottedNotationDict


class TestNotation:
    def test_initial(self):
        t = DottedNotationDict(
            {
                "1": {
                    "2": {"3": "4", "5": [33]},
                },
                "list": [{"6": 7}],
            }
        )

        assert t["1.2.3"] == "4"
        assert t["1.3"] is None
        assert t["1.2.5.[0]"] == 33
        assert t["1.2.5"] == [33]
        assert t["1.2.5."] is None
        assert t["list.[0].6"] == 7

    def test_list(self):
        t = DottedNotationDict()
        t["middle_0.[0].a.[0].[0].a.[0]"] = 50
        assert t["middle_0.[0].a.[0].[0].a"] == [50]
        # list in the middle
        t["middle_1.[0].b"] = 55
        t["middle_1.[0].b.c"] = 56
        t["middle_1.[0].b.d"] = 57
        assert t["middle_1"] == [{"b": 55}]
        t["middle_1"].append({})
        t["middle_1.[1].c"] = 60
        assert len(t["middle_1"]) == 2
        assert t["middle_1.[0]"] == {"b": 55}
        assert t["middle_1.[1]"] == {"c": 60}
        assert t["middle_1"] == [{"b": 55}, {"c": 60}]
        t["middle_2.[0].b.c"] = 56
        assert t["middle_2"][0]["b"]["c"] == 56

        # list share object on same index
        t["middle_30.[0].a"] = 56
        t["middle_30.[0].b"] = 57
        assert t["middle_30"] == [{"a": 56, "b": 57}]

        # list insert new object after index
        t["middle_31.[0].a"] = 56
        t["middle_31.[1].b"] = 57
        t["middle_31.[1].c"] = 58
        assert t["middle_31"] == [{"a": 56}, {"b": 57, "c": 58}]

        # list in the middle, multiple
        t["middle_4.[0].[0].e"] = 155
        assert t["middle_4"][0][0]["e"] == 155
        t["middle_4.[0].[0].f.c"] = 156
        assert t["middle_4"][0][0]["f"]["c"] == 156
        t["middle_4.[0].[0].f.f"] = 560
        assert t["middle_4"][0][0]["f"]["f"] == 560

        # list at the end
        t["end_1.[0]"] = 1555
        assert t["end_1"][0] == 1555
        t["end_1.[0]"] = 15555
        assert t["end_1"] == [15555, 1555]
        t["end_2.[0].[0]"] = 15555
        assert t["end_2"][0][0] == 15555

        # list at start
        t["middle_31.[0].a"] = 56
        t["middle_31.[1].b"] = 57
        t["middle_31.[1].c"] = 58
        t["middle_31.[3].c"] = 59
        assert t["middle_31"] == [{"a": 56}, {"b": 57, "c": 58}, None, {"c": 59}]
        t["middle_31.[2].d"] = 60
        assert t["middle_31"] == [{"a": 56}, {"b": 57, "c": 58}, {"d": 60}, {"c": 59}]

    def test_custom(self):
        t = DottedNotationDict()
        t["references.supplyChain.[0].aux.legacyDatasourceId"] = 5
        t["references.supplyChain.[0].id"] = 10
        t["references.supplyChain.[0].sourceComment"] = "hi"
        t["references.supplyChain.[0].aux.warehouseId"] = 15
        assert t["references.supplyChain"] == [
            {
                "id": 10,
                "sourceComment": "hi",
                "aux": {
                    "legacyDatasourceId": 5,
                    "warehouseId": 15,
                },
            }
        ]

    def test_overwrite(self):
        t = DottedNotationDict()
        t["5"] = 6
        assert t["5"] == 6
        t["5"] = 7
        assert t["5"] == 7
        t["5.6"] = 6
        assert t["5.6"] == None
        t["5.6"] = 7
        assert t["5.6"] == None
        t["6.6"] = 6
        assert t["6.6"] == 6
        t["6.6"] = 7
        assert t["6.6"] == 7
        t["7.[0]"] = 7
        assert t["7"] == [7]


if __name__ == "__main__":
    pytest.main([__file__])
