libname NW_CDSM oracle user=NW_CDSM PASSWORD='{SAS002}F7A6FA3F2523740838AA203A18B8FAAF' PATH='@NWDW' schema='NW_CDSM';
libname CO_LBOLT oracle user=NW_CDSM PASSWORD='{SAS002}F7A6FA3F2523740838AA203A18B8FAAF' PATH='@NWDW' schema='CO_LBOLT';
libname CO_LB_DM '/apps/sas/datasets/data12/NWPERMA/Finance/CO_DM_Dev';

/* Date Span */
%let start_date=25Dec2021;
%let end_date=18Feb2022;

/* Final Summary Pay Periods */
%let fin_sum_m2pp_start=25Dec2021;
%let fin_sum_m2pp_end=07Jan2022;
%let fin_sum_m1pp_start=08Jan2022;
%let fin_sum_m1pp_end=21Jan2022;

/* Prospective Summary Pay Periods*/
%let pro_sum_p1pp_start=22Jan2022;
%let pro_sum_p1pp_end=04Feb2022;
%let pro_sum_p2pp_start=05Feb2022;
%let pro_sum_p2pp_end=18Feb2022;


data fin_sum_DATE_RANGE;
/*do date=%SYSFUNC(intnx(WEEK.1,%SYSFUNC(today()),-4)) to %SYSFUNC(intnx(WEEK.7,%SYSFUNC(today()),-0));*/
do DATE="&fin_sum_m2pp_start"d to "&fin_sum_m1pp_end"d;
output;
end;
format DATE date9.;
run;


PROC SQL;
	CREATE TABLE fin_sum_DM_PP_DATA AS
		SELECT DISTINCT
			SLOT_DATE,
			NUID,
			LAST_NM,
			FIRST_NM,
			catx(' ',FIRST_NM,LAST_NM) as FULL_NM,
			ASSIGN_DEPT_NM,
			payrl_cd,
			AVG(FTE_HRS)/13 AS PP_FTE_HRS format=6.2,
			SUM(NET_HOURS) AS NET_HOURS format=6.2
		FROM CO_LB_DM.PAYRL_DATASET_1225_TO_0218
		WHERE NUID = 'Z707952'
			AND DATEPART(SLOT_DATE) >= "&fin_sum_m2pp_start"d
			AND DATEPART(SLOT_DATE) <= "&fin_sum_m1pp_end"d
		GROUP BY NUID, SLOT_DATE, payrl_cd
		ORDER BY NUID, SLOT_DATE
;
QUIT;

PROC SQL;
	CREATE TABLE fin_sum_DM_PP_DATA_SUMMARY AS
		SELECT DISTINCT
			NUID,
			LAST_NM,
			FIRST_NM,
			FULL_NM,
			AVG(PP_FTE_HRS) AS PP_FTE_HRS format=6.2,
			SUM(NET_HOURS) AS NET_HOURS format=6.2
		FROM fin_sum_DM_PP_DATA PAYRL
		WHERE NUID = 'Z707952'
		GROUP BY NUID
		ORDER BY NUID
;
QUIT;
/* NET_HOURS Calc */
data fin_sum_DM_PP_DATA_SUMMARY;
	set fin_sum_DM_PP_DATA_SUMMARY;
	TRUE_UP_CALC = NET_HOURS - PP_FTE_HRS;
	IF TRUE_UP_CALC <0 THEN TRUE_UP_STATUS = 'TRUE_UP_NEGATIVE';
	IF TRUE_UP_CALC >0 THEN TRUE_UP_STATUS = 'TRUE_UP_POSITIVE';
	IF TRUE_UP_CALC =0.00 THEN TRUE_UP_STATUS = 'TRUE_UP_NEUTRAL';
	FORMAT TRUE_UP_CALC 6.2;
run;


PROC SQL;
	CREATE TABLE fin_sum_DM_PP_PRIMER AS
		SELECT DISTINCT
			DATE,
			NUID,
			LAST_NM,
			FIRST_NM
		FROM CO_LB_DM.PAYRL_DATASET_1225_TO_0218 PAYRL
			LEFT JOIN fin_sum_DATE_RANGE ON PAYRL.NUID IS NOT NULL
		WHERE NUID = 'Z707952'
			AND DATEPART(SLOT_DATE) >= "&fin_sum_m2pp_start"d
			AND DATEPART(SLOT_DATE) <= "&fin_sum_m1pp_end"d
		GROUP BY NUID, DATE
		ORDER BY NUID
;
QUIT;


PROC SQL;
	CREATE TABLE fin_sum_DM_PP_DATE_RANGE AS
		SELECT
			DM_PP_PRIMER.*,
			DM_PP_DATA.FULL_NM,
			DM_PP_DATA.ASSIGN_DEPT_NM,
			DM_PP_DATA.payrl_cd,
			DM_PP_DATA.NET_HOURS
		FROM fin_sum_DM_PP_PRIMER DM_PP_PRIMER
			LEFT JOIN fin_sum_DM_PP_DATA DM_PP_DATA
				ON DATEPART(fin_sum_DM_PP_DATA.SLOT_DATE) = fin_sum_DM_PP_PRIMER.date
				AND DM_PP_PRIMER.NUID = DM_PP_DATA.NUID
		ORDER BY date, NUID
;
QUIT;

proc sql noprint;
	select LAST_NM
	into: prov
	from fin_sum_DM_PP_DATA_SUMMARY;
quit;

proc sql noprint;
	select PP_FTE_HRS
	into: bench
	from fin_sum_DM_PP_DATA_SUMMARY;
quit;

proc transpose data=fin_sum_DM_PP_DATA_SUMMARY out=summary_rotate(drop=NUID FULL_NM);
	BY NUID;
	VAR NET_HOURS 
		TRUE_UP_STATUS
		PP_FTE_HRS 
		TRUE_UP_CALC;
run;


%put &prov.;
%put &start_date.;
%put &end_date.;
%put &bench.;

%let hdate = %sysfunc(today(),mmddyyp.);
%put hdate = &hdate;

%let hdates = %sysfunc(today(),mmddyys10.);
%put hdates = &hdates;

ods listing close;
ods pdf file="/apps/sas/datasets/data12/NWPERMA/Finance/DevSWS/Summary_Sample.pdf" notoc uniform startpage=never;
ods escapechar='^';
options nodate nonumber spool;




/*Print Pay Period Summary*/
/*proc report data=fin_sum_DM_PP_DATA_SUMMARY;*/
proc report data=summary_rotate;
title j=l "^{style[preimage='/apps/sas/datasets/data12/NWPERMA/Finance/DevMB/cpmg_label.png'] } "
      j=r "^{style[vjust=r]Final Summary: Dr. &prov.}";

/*Set Columns*/
/*columns NUID */
/*		FULL_NM */
/*		pp_fte_hrs */
/*		net_hours */
/*		true_up_calc */
/*		true_up_status;*/

columns _name_
		col1;

/*Format Columns*/
/*define NUID / display 'NUID' style(column)={width=1.25in};*/
/*define FULL_NM / display 'Name' style(column)={width=1.25in};*/
/*define pp_fte_hrs / display 'FTE Benchmark' style(column)={width=1.25in};*/
/*define net_hours / display 'Net Hours' analysis style(column)={width=1.25in};*/
/*define true_up_calc / display 'True Up Needed' style(column)={width=.75in};*/
/*define true_up_status / display 'True Up Status' style(column)={width=.75in};*/

define _name_ / display ' ' style(column)={width=2in};
define col1 / display 'Hours' style(column)={width=2in};
run;





/*Print Pay Period Details*/
proc report data=fin_sum_DM_PP_DATE_RANGE;

columns DATE
		ASSIGN_DEPT_NM
		PAYRL_CD
		NET_HOURS;
	
define DATE / display 'DATE' style(column)={width=1.25in};
define NUID / group display 'NUID' style(column)={width=1.25in};
define ASSIGN_DEPT_NM / display 'ASSIGN_DEPT_NM' style(column)={width=1.25in};
define PAYRL_CD / display 'PAYRL_CD' style(column)={width=1.25in};
define NET_HOURS / display analysis sum 'NET_HOURS' style(column)={width=1.25in};
rbreak after / summarize style=[font_weight=bold];

footnote '^S={postimage="/apps/sas/datasets/data12/NWPERMA/Finance/DevMB/kp_label.png" just=r}';
run;
ods pdf close;
ods listing;
		
filename outmail email	

To="Scott W. Steinbrueck <Scott.W.Steinbrueck@kp.org>"
From="Scott W. Steinbrueck <Scott.W.Steinbrueck@kp.org>"
Sender="Scott W. Steinbrueck <Scott.W.Steinbrueck@kp.org>"
/*cc=("Michael A Bernard <Michael.A.Bernard@kp.org>" "Henry T Burmeister <henry.t.burmeister@kp.org>" "Scott W. Steinbrueck <Scott.W.Steinbrueck@kp.org>")*/
subject="Summary Sample"
attach=("/apps/sas/datasets/data12/NWPERMA/Finance/DevSWS/Summary_Sample.pdf");
data _null_;    
   file outmail;    
   put ' ';   
   put "Summary Sample.";
   put ' ';
   put 'Please email Scott Steinbrueck (scott.w.steinbrueck@kp.org) if you have questions.'; 
   put ' ';   
   ************ This is your Signature information **************;

    put 'Thank You,';
    put 'Michael Bernard';
    put 'Northwest Permanente';
	put 'Kaiser Permanente Northwest';
	put 'E-mail: michael.a.bernard@kp.org';
	put ' ';
run;

	