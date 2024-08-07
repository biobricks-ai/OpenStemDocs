from datetime import datetime

today = datetime.date(datetime.today())

if __name__ == '__main__':
    if today.day < 30:
        last_month = today.replace(month=today.month - 1, day=30)
        if today.month == 1:
            last_month = last_month.replace(year=last_month.year - 1)
        print(last_month)