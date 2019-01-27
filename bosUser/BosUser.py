from s3db import s3DB
import json
from datetime import date, datetime, time

class BosUser:
    days_total = 21
#{"user_id": "<id>", "name": "<name>", "email": "<email>", "image": "<image>", "active": true, "approvers":
#[...], "days_total": 21, "years":
#   [{"2017":{"days_leftover": 3, "days_remaining": 5, "days_total": 21}},
#   {"2018":{"days_leftover": 0, "days_remaining": 21, "days_total": 28}}
#   ]}
    def __init__(self, user_data, year):

        self.name = user_data['username']
        self.email = user_data['email']
        self.image = user_data['image']
        self.user_id = user_data['principalId']
        self.entries = BosDays(self.name, year)
        existing_data = self.load(self.user_id)
        if existing_data:
            self.active = existing_data.active
            self.approvers = existing_data.approvers
            self.days_total = existing_data.days_total
            if year in existing_data.years:
                self.years = existing_data.years
            else:
                new_year = {year: {"days_leftover": existing_data[str(year)-1]['days_remaining'],
                                    "days_remaining": self.days_total,
                                    "days_total": self.days_total}}
                self.years.append(new_year)
        else:
            self.active = False
            self.approvers = []
            self.days_total = BosUser.days_total
            self.years = [{year: {"days_leftover": self.days_total, "days_remaining": self.days_total, "days_total": self.days_total}}]

        self.saveUserData()

    def __str__(self):
        user_dict = dict(self.toJSON())
        user_dict.pop('entries')
        entries_data = self.entries.toJSON()
        user_dict['entries'] = entries_data
        return json.dumps(user_dict)

    def toJSON(self):
        user_data =  dict(self.__dict__)
        entries_data = user_data.pop('entries')
        user_data['entries'] = entries_data.toJSON()
        return user_data

    def saveUserData(self):
        mydb = s3DB.s3DB('users')
        mydb.bucket = "tbos-data"
        mydb.index = "user_id"
        user = dict(self.toJSON())
        user.pop('entries')
        mydb.save(user)

    def load(self, user_id):
        mydb = s3DB.s3DB('users')
        mydb.bucket = "tbos-data"
        user = mydb.load(user_id)
        return user



    def addEntry(self, date_start, date_end, type):
        pass

class BosDays:
    days_total = 21

    def __init__(self, user_id, year):
        entries = self.load(user_id, year)
        self.days_taken = entries['1']
        self.halfdays_taken = entries['2']
        self.sick_days = entries['3']
        self.holiday_requests = entries['4']
        self.work_requests = entries['5']
        self.training = entries['6']
        self.workon_days = entries['7']

    def __str__():
        days_dict = self.__dict__
        return json.dumps(days_dict)

    def toJSON(self):
        return self.__dict__

    def save(self, user_id, year):
        mydb = s3DB.s3DB('entries')
        mydb.bucket = "tbos-data"
        mydb.index = "year"
        data = mydb.load(year)
        if data:
            data[user_id] = self.toJSON()
        else:
            data = {'year': year, 'months': []}
        mydb.save(data)

    def load(self, user_id, year):
        mydb = s3DB.s3DB('entries')
        mydb.bucket = "tbos-data"
        all_entries = mydb.load(year)
        user_entries = {"1": [], "2": [], "3": [], "4": [], "5": [], "6": [], "7": []}
        if all_entries:
            if all_entries['months']:
                for month in all_entries['months']:
                    current_month = month['month']
                    for day in month['days']:
                        current_day = day['day']
                        for entry in day['entries']:
                            if entry['user_id'] == user_id:
                                user_entries[entry['type']].append(date(year, current_month, current_day).strftime("%Y-%m-%d"))
        return user_entries

        #datastructure
        #{"year":'2018', "months":
        #   [{"month": "1", "days":
        #
        #       [{"day": "01",
        #           "entries":
        #               [{"user_id": "<id>", "type": "1"},
        #               {"user_id": "<id>", type": "2"}]
        #
        #        },
        #        {"day": "02", "entries": [...]}
        #       }]
        #
        #       },
        #   {"month": "2", "days": [...]}
        #   }]
        #}


class BosYear:
#datastructure
#{"year":2018, "months":
#   [{"month": "1", "days":
#
#       [{"day": "01",
#           "entries":
#               [{"user_id": "<id>", "type": "1"},
#               {"user_id": "<id>", type": "2"}]
#
#        },
#        {"day": "02", "entries": [...]}
#       }]
#
#       },
#   {"month": "2", "days": [...]}
#   }]
#}
    def __init__(self, year):
        year_data = self.load(year)
        if year_data:
            self.year = year_data['year']
            self.months = year_data['months']
        else:
            self.year = str(datetime.now().year)
            self.months = []

    def load(self, year):
        mydb = s3DB.s3DB('entries')
        mydb.bucket = "tbos-data"
        entries = mydb.load(year)
        return entries

    def toJSON(self):
        return self.__dict__

    def __str__(self):
        return json.dumps(self.toJSON())

    def addEntry(self, start_date, end_date, user):
        pass

    def deleteEntry(self, date, user):
        pass


class AllUsers:

    def __init__(self):
        user_list = self.load()
        if user_list:
            self.index = user_list['index']
            self.all_users = user_list['all_users']
        else:
            self.index = 'all_users'
            self.all_users = []

    def toJSON(self):
        return self.__dict__

    def __str__(self):
        return json.dumps(self.toJSON())

    def addUser(self, user):
        if user:
            if not any(d['user_id'] == user.user_id for d in self.all_users):
                self.all_users.append({'name':user.name, 'user_id': user.user_id, 'email': user.email})
                self.save()

    def load(self):
        mydb = s3DB.s3DB('all_users')
        mydb.bucket = "tbos-data"
        user_list = mydb.load('all_users')
        return user_list

    def save(self):
        mydb = s3DB.s3DB('all_users')
        mydb.bucket = "tbos-data"
        mydb.index = 'index'
        mydb.save(self.toJSON())
