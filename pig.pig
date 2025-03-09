claims = load 'train.csv' using PigStorage(',') as (
    ClaimNumber:chararray, DateTimeOfAccident:chararray, DateReported:chararray, Age:int, Gender:chararray,
       MaritalStatus:chararray, DependentChildren:int, DependentsOther:int, WeeklyWages:float,
       PartTimeFullTime:chararray, HoursWorkedPerWeek:float, DaysWorkedPerWeek:int,
       ClaimDescription:chararray, InitialIncurredCalimsCost:int, UltimateIncurredClaimCost:int);

claims_clean = foreach claims generate ClaimNumber..Gender,
      ((MaritalStatus IS  NULL) ? 'U' : MaritalStatus), DependentChildren, DependentsOther, ((WeeklyWages IS  NULL) ? 0 : WeeklyWages),
      PartTimeFullTime,((HoursWorkedPerWeek IS  NULL) ? 0 : HoursWorkedPerWeek) , DaysWorkedPerWeek..UltimateIncurredClaimCost
    ;


STORE claims_clean INTO 'nulos' using PigStorage(',');