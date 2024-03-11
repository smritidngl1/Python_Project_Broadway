import csv


def connection():
    import psycopg2
    conn = psycopg2.connect(
        database ="surveydb",
        user =  "postgres",
        password = "123",
        host = "127.0.0.1",
        port = 5432
    )

    conn.autocommit = True
    print("Connection successfully established!!!")
    cursor = conn.cursor()
    return cursor

cursor = connection()


Create_Table_SQL = """
    CREATE TABLE surveyresult (
        Respondent INT PRIMARY KEY,
        MainBranch TEXT,
        Hobbyist TEXT,
        OpenSourcer TEXT,
        OpenSource TEXT,
        Employment TEXT,
        Country TEXT,
        Student TEXT,
        EdLevel TEXT,
        UndergradMajor TEXT,
        EduOther TEXT,
        OrgSize TEXT,
        DevType TEXT,
        YearsCode TEXT,
        Age1stCode TEXT,
        YearsCodePro TEXT,
        CareerSat TEXT,
        JobSat TEXT,
        MgrIdiot TEXT,
        MgrMoney TEXT,
        MgrWant TEXT,
        JobSeek TEXT,
        LastHireDate TEXT,
        LastInt TEXT,
        FizzBuzz TEXT,
        JobFactors TEXT,
        ResumeUpdate TEXT,
        CurrencySymbol TEXT,
        CurrencyDesc TEXT,
        CompTotal TEXT,
        CompFreq TEXT,
        ConvertedComp TEXT,
        WorkWeekHrs TEXT,
        WorkPlan TEXT,
        WorkChallenge TEXT,
        WorkRemote TEXT,
        WorkLoc TEXT,
        ImpSyn TEXT,
        CodeRev TEXT,
        CodeRevHrs TEXT,
        UnitTests TEXT,
        PurchaseHow TEXT,
        PurchaseWhat TEXT,
        LanguageWorkedWith TEXT,
        LanguageDesireNextYear TEXT,
        DatabaseWorkedWith TEXT,
        DatabaseDesireNextYear TEXT,
        PlatformWorkedWith TEXT,
        PlatformDesireNextYear TEXT,
        WebFrameWorkedWith TEXT,
        WebFrameDesireNextYear TEXT,
        MiscTechWorkedWith TEXT,
        MiscTechDesireNextYear TEXT,
        DevEnviron TEXT,
        OpSys TEXT,
        Containers TEXT,
        BlockchainOrg TEXT,
        BlockchainIs TEXT,
        BetterLife TEXT,
        ITperson TEXT,
        OffOn TEXT,
        SocialMedia TEXT,
        Extraversion TEXT,
        ScreenName TEXT,
        SOVisit1st TEXT,
        SOVisitFreq TEXT,
        SOVisitTo TEXT,
        SOFindAnswer TEXT,
        SOTimeSaved TEXT,
        SOHowMuchTime TEXT,
        SOAccount TEXT,
        SOPartFreq TEXT,
        SOJobs TEXT,
        EntTeams TEXT,
        SOComm TEXT,
        WelcomeChange TEXT,
        SONewContent TEXT,
        Age TEXT,
        Gender TEXT,
        Trans TEXT,
        Sexuality TEXT,
        Ethnicity TEXT,
        Dependents TEXT,
        SurveyLength TEXT,
        SurveyEase TEXT
    )
"""
cursor.execute(Create_Table_SQL)
print("Table 'surveyresult' created successfully!")


filename = "jupyter_task/survey_results_public.csv"


with open(filename, "r", encoding='utf-8') as cr:
    surveyresult = csv.DictReader(cr)
    for count, surveyresult in enumerate(surveyresult):
        if count >= 50:
            break

        sql = f"""
            INSERT INTO survey ({', '.join(surveyresult.keys())})
            VALUES ({', '.join(['%s']*len(surveyresult))})
        """
        cursor.execute(sql, list(surveyresult.values()))

    print("Rows are successfully inserted!!!")

cursor.close()
