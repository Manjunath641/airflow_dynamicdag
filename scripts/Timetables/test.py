import holidays

year = 2023
def get_australian_holidays():
    au_holidays = holidays.US()
    print(au_holidays)
    #return list(au_holidays.keys())