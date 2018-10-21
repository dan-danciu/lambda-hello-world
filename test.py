import json

class Test:
    def __init__(self, value1, value2):
        self.value1 = value1
        self.value2 = value2
        dob = Duda("trololo", "xyz", [{"one":1},
                                    {"two":2},
                                     3])
        self.dob = dob
        self.printTestData()

    def __str__(self):
        test_data = dict(self.testObj())
        test_data.pop('dob')
        duda_data = self.dob.dudaObj()
        test_data['dob'] = duda_data
        return json.dumps(test_data)

    def testObj(self):
        return self.__dict__

    def printTestData(self):
        tob = dict(self.testObj())
        tob.pop('dob')
        print(f'without popped duda: {tob}')

class Duda:
    def __init__(self, duda1, duda2, duda3):
        self.duda1 = duda1
        self.duda2 = duda2
        self.duda3 = duda3

    def dudaObj(self):
        return self.__dict__

    def __str__(self):
        return json.dumps(self.__dict__)

s = Test(123, "abc")
print(s)
print(s.value1)
print(s.value2)
print(s.dob)
print(type(s.dob.dudaObj()['duda3'][0]))
