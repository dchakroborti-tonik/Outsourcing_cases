drop table if exists `prj-prod-dataplatform.dap_ds_poweruser_playground.tempdata`;
create table `prj-prod-dataplatform.dap_ds_poweruser_playground.tempdata` as 
with 
segmentc as (select loanAccountNumber from `risk_credit_mis.loan_master_table`
where coalesce(currentDelinquency, 0) > 90
and loanAccountNumber not in (select distinct loanAccountNumber
from 
(select loanAccountNumber, mobileNumber, PTP_Kept, Payment_Receiving_Date from `risk_credit_mis.collection_calling_master_full` where mobileNumber is not null and campaignname in ('PROD_Pre-Collections',
      'Special_Pre-Collections', 'PROD_Hard Collections', 'Special Campaign_Hard Collections', 'PROD_Soft Collections_Daily',
    'Special Campaign_Soft Collections', 'SIP_Prod_Reminder', 'Collection HaRD PTPr', 'Collection Hard BROKEN PTP', 'Collections_Soft PTPr')
    and loanAccountNumber is not null 
    and PTP_Kept = 1
    and date_trunc(Payment_Receiving_Date, day) between date_sub(current_date(), interval 60 day) and current_date()
))
and loanAccountNumber not in (select distinct loanAccountNumber from 
(select loanAccountNumber, lastPaymentDate from `risk_credit_mis.loan_installments_table`
where date(coalesce(lastPaymentDate, '3000-01-01')) between date_sub(current_date(), interval 60 day) and current_date()
))),
segmentb as (select * from `risk_credit_mis.loan_master_table`
where coalesce(currentDelinquency, 0) > 60 and coalesce(currentDelinquency, 0) <= 90
and loanAccountNumber not in (select lit.loanAccountNumber from `risk_credit_mis.loan_installments_table` lit 
inner join (select loanAccountNumber, max(installmentDueDate) latestinstallmentduedate, max(installmentNumber) latestinstallmentnumber
 from `risk_credit_mis.loan_installments_table` where installmentDueDate <= current_date()
 group by loanAccountNumber) i on i.loanAccountNumber = lit.loanAccountNumber and installmentDueDate = latestinstallmentduedate and lit.installmentNumber = i.latestinstallmentnumber
where coalesce(directDebitStatus, 'NA') not like 'PARTIALPAYMENT') -- no partial payment
and (loanAccountNumber in (select distinct loanAccountNumber from `risk_credit_mis.collection_calling_master_full`where connected = 0 and mobileNumber is not null
and date_trunc(callDatetime, day) between date_sub(current_date(), interval 30 day) and current_date()
and loanAccountNumber is not null)
or 
loanAccountNumber in (select distinct loanAccountNumber  from `risk_credit_mis.collection_calling_master_full`where connected = 0 and mobileNumber is not null
and (lower(subdisposition) like '%refuse%' or lower(genesysWrapupDisposition) like '%refuse%')
and loanAccountNumber is not null)
)
-- no contact in last30days
and loanAccountNumber not in (select distinct c.loanAccountNumber from `risk_credit_mis.collection_calling_master_full` c
inner join (select loanAccountNumber, min(bucketDate) bucketdate, min(Max_current_DPD) mindpd from `risk_credit_mis.loan_bucket_flow_report_core` where Max_current_DPD between 61 and 90
group by loanAccountNumber
) b on b.loanAccountNumber = c.loanAccountNumber
where c.mobileNumber is not null and c.campaignname in ('PROD_Pre-Collections',
      'Special_Pre-Collections', 'PROD_Hard Collections', 'Special Campaign_Hard Collections', 'PROD_Soft Collections_Daily',
    'Special Campaign_Soft Collections', 'SIP_Prod_Reminder', 'Collection HaRD PTPr', 'Collection Hard BROKEN PTP', 'Collections_Soft PTPr')
    and c.loanAccountNumber is not null and c.PTP_Date is not null
and date_trunc(c.PTP_Date, day) between b.bucketdate and current_date()) -- no ptp from client
),
segmenta as (select loanAccountNumber , currentDelinquency
, CASE
      WHEN loanType = 'BNPL' AND purpleKey IS NOT NULL AND (isUserAtStore = 1 OR isUserAtStore IS NULL) THEN 'SIL'
      WHEN loanType = 'BNPL'
    AND purpleKey IS NOT NULL
    AND isUserAtStore = 0 THEN 'SIL-Online'
      WHEN loanType = 'BNPL' AND purpleKey IS NULL THEN 'SIL-Online'
      WHEN loanType = 'TSBL' THEN 'QL FLEX'
      WHEN loanType = 'TUL' THEN 'QL BO'
    ELSE
    loanType
  END
    ltype
from `risk_credit_mis.loan_master_table`
where coalesce(currentDelinquency, 0) > 20 and coalesce(currentDelinquency, 0) <= 60
and loanAccountNumber in (select loanAccountNumber from `risk_credit_mis.loan_master_table` where (obsFPD00 = 1 or obsSPD00 = 1) and obsTPD00 = 0) -- FPD and SPD
and loanAccountNumber not in (select lit.loanAccountNumber from `risk_credit_mis.loan_installments_table` lit 
inner join (select loanAccountNumber, max(installmentDueDate) latestinstallmentduedate, max(installmentNumber) latestinstallmentnumber
 from `risk_credit_mis.loan_installments_table` where installmentDueDate <= current_date()
 group by loanAccountNumber) i on i.loanAccountNumber = lit.loanAccountNumber and installmentDueDate = latestinstallmentduedate and lit.installmentNumber = i.latestinstallmentnumber
where coalesce(directDebitStatus, 'NA') not like 'PARTIALPAYMENT') -- no partial payment
and loanAccountNumber in (select loanAccountNumber
from 
(
select c.loanAccountNumber, c.callDatetime, b.bucketDate, b.Max_current_DPD from 
(select loanAccountNumber, bucketDate, Max_current_DPD from `risk_credit_mis.loan_bucket_flow_report_core` where Max_current_DPD between 1 and 20
) b
inner join (
select loanAccountNumber, callDatetime from `risk_credit_mis.collection_calling_master_full` where mobileNumber is not null and campaignname in ('PROD_Pre-Collections',
      'Special_Pre-Collections', 'PROD_Hard Collections', 'Special Campaign_Hard Collections', 'PROD_Soft Collections_Daily',
    'Special Campaign_Soft Collections', 'SIP_Prod_Reminder', 'Collection HaRD PTPr', 'Collection Hard BROKEN PTP', 'Collections_Soft PTPr')
    and loanAccountNumber is not null 
) c on b.loanAccountNumber = c.loanAccountNumber and date_trunc(b.bucketDate, day) = date_trunc(c.callDatetime, day)
order by 1,2,3,4
)
group by loanAccountNumber having count(loanAccountNumber) > 10)
and loanAccountNumber in (select loanAccountNumber from 
(select loanAccountNumber, subdisposition, genesysWrapupDisposition, campaignName
, case when upper(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%COLLECTION%' then 1 
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DELAY%' then 1 
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DUE%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like 'PAYMENT%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%WRAP%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%HANG%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PTP REMINDER%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%UNSUCCESSFUL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%CALL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE TO PAY%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DROP CALL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PAID%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DEAD AIR%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%3RD PARTY%' then 1 else 0 end APC,
  case when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%COLLECTION%' then 1 
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DELAY%' then 1 
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DUE%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%HANG%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like 'PAYMENT%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PTP REMINDER%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%CALL BACK%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE TO PAY%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DROP CALL%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PAID%' then 1 else 0 end RPC,
  case when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%3RD PARTY%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%UNSUCCESSFUL%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DEAD AIR%' then 1
       else 0 end TPC,
  callDatetime, connected
 from `risk_credit_mis.collection_calling_master_full` ccmf
where campaignName in
('WCL MARCH 3',
'WCL FINAL SPIN 2',
'WCL PART 2 2072022',
'SIP_Pre-Collections',
'Testing_Collections',
'PROD_Pre-Collections',
'Collections_Soft PTPr',
'OB WCL July 18 Spin 2',
'OB WCL July 18 Spin 3',
'OB WCL July 26 spin 3',
'OB WCL Spin 1 July 18',
'PROD_Hard Collections',
'WCL 2 - Feb 21 Spin 2',
'Special_HaRD_Collection',
'WCL 2 - 1st Spin Feb 24',
'WCL 3rd Attempt 1302022',
'WCL FEB 18 to 20 Spin 2',
'WCL - 1st Attempt 222022',
'WCL - 1st Attempt 282022',
'WCL - 1st attempt 212022',
'WCL 2 - FEB 22 - 3RD SPIN',
'WCL 2 Feb 24 to 27 SPIN 3',
'Collection Hard BROKEN PTP',
'PROD_Soft Collections_Daily',
'Collections_Possible Fraud.2',
'FEB 16 - SPIN 1 - WCL Skill 2',
'Special Campaign_WCL_Collection',
'Special Campaign_Pre Collections',
'Collections-Daily Outbound_Manual',
'Collections_Special_Pre-Col_May30',
'Collections_normalization special',
'Special Campaign_Hard Collections',
'Special Campaign_Soft Collections',
'Tonik Agentless -Collection',
'WCL 2 - FEB 22 - SPIN 3',
'WCL - 1st Attempt 1302022',
'WCL - 2nd Attempt 1302022',
'WCL Skill 2 - Feb 11 - 1st Spin',
'WCL 2 Spin 3 2192022',
'WCL 2nd attempt 212022',
'WCL 2 - 2nd SPIN Feb 24',
'WCL - 3rd attempt 282022',
'FEB 16 - WCL 2 - 3rd Spin',
'WCL 2 - 1ST SPIN - FEB 22',
'WCL 2 - 3rd SPIN - FEB 23',
'WCL 2 - FEB 17 - 2nd Spin',
'WCL 2 - FEB 18 - 1ST SPIN',
'WCL SKill 2 - feb 10 - 3rd spin',
'WCL Skill 2 - 2nd Spin - Feb 23',
'WCL 2nd Attempt -222022',
'WCL 2 - FEB 24 - 3rd SPIN',
'WCL SKILL 2 - FEB 10 2022 - 2nd SPIN',
'Collection HaRD PTPr')
and coalesce(connected, 0) = 0
)) -- no contact with client
),
loansdata as 
(select customerId, loanAccountNumber, disbursementDateTime  
, firstName, middleName, lastName
, concat(firstName, ' ', middleName, ' ', lastName) clientName
, province
, coalesce(currentDelinquency, 0) dpd
, Overdue_Principal
, Overdue_Interest
, Overdue_Penalty
, coalesce(Overdue_Principal, 0) + coalesce(Overdue_Interest, 0) + coalesce(Overdue_Penalty, 0) total_overdue_amount
, outstandingBalance
, coalesce(Total_Outstanding_Principal, 0) + coalesce(Total_Outstanding_Interest, 0) + coalesce(Total_Outstanding_Penalty) total_outstanding_amount
, mobileNo
,     CASE
      WHEN loanType = 'BNPL' AND purpleKey IS NOT NULL AND (isUserAtStore = 1 OR isUserAtStore IS NULL) THEN 'SIL'
      WHEN loanType = 'BNPL'
    AND purpleKey IS NOT NULL
    AND isUserAtStore = 0 THEN 'SIL-Online'
      WHEN loanType = 'BNPL' AND purpleKey IS NULL THEN 'SIL-Online'
      WHEN loanType = 'TSBL' THEN 'QL FLEX'
      WHEN loanType = 'TUL' THEN 'QL BO'
    ELSE
    loanType
  END
    ltype
,lastPaidDT,
from `risk_credit_mis.loan_master_table` where flagDisbursement = 1 and coalesce(currentDelinquency, 0) > 0),
instdata as (select loanAccountNumber, min(installmentNumber)minunpaidinstallmentnumber from `risk_credit_mis.loan_installments_table` where isCurrentDelinquent = 1 group by loanAccountNumber),
cd as (select customer_id, new_mobile_number, row_number() over(partition by customer_id order by change_date)rnk from `risk_credit_mis.customer_contact_details` where ACTIVE = 'Y'),
rpcdata as (select loanAccountNumber, RPC, max(date_trunc(callDatetime, day)) lastrpcdate from (select loanAccountNumber, mobileNumber, new_mobile_number, callDatetime
, case when upper(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%COLLECTION%' then 1 
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DELAY%' then 1 
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DUE%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like 'PAYMENT%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%WRAP%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%HANG%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PTP REMINDER%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%UNSUCCESSFUL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%CALL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE TO PAY%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DROP CALL%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PAID%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DEAD AIR%' then 1
     when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%3RD PARTY%' then 1 else 0 end APC,
  case when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%COLLECTION%' then 1 
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DELAY%' then 1 
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DUE%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%HANG%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like 'PAYMENT%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PTP REMINDER%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%CALL BACK%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PROMISE TO PAY%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DROP CALL%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%PAID%' then 1 else 0 end RPC,
  case when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%3RD PARTY%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%UNSUCCESSFUL%' then 1
       when UPPER(coalesce(ccmf.genesysWrapupDisposition, 'NA')) like '%DEAD AIR%' then 1
       else 0 end TPC from `risk_credit_mis.collection_calling_master_full` ccmf 
       left join cd on cd.customer_id = ccmf.customerId
       where mobileNumber is not null and campaignname in ('PROD_Pre-Collections',
      'Special_Pre-Collections', 'PROD_Hard Collections', 'Special Campaign_Hard Collections', 'PROD_Soft Collections_Daily',
    'Special Campaign_Soft Collections', 'SIP_Prod_Reminder', 'Collection HaRD PTPr', 'Collection Hard BROKEN PTP', 'Collections_Soft PTPr') )where loanAccountNumber is not null and RPC = 1 group by loanAccountNumber, RPC),
lastptp as (select * from 
(select loanAccountNumber, max(PTP_Date) lastptpdate from `risk_credit_mis.collection_calling_master_full`
where mobileNumber is not null and campaignname in ('PROD_Pre-Collections',
      'Special_Pre-Collections', 'PROD_Hard Collections', 'Special Campaign_Hard Collections', 'PROD_Soft Collections_Daily',
    'Special Campaign_Soft Collections', 'SIP_Prod_Reminder', 'Collection HaRD PTPr', 'Collection Hard BROKEN PTP', 'Collections_Soft PTPr')
    and PTP_Date is not null
    group by loanAccountNumber
)
where loanAccountNumber is not null),
md as  
(
select loansdata.customerId
, cast(loansdata.loanAccountNumber as string) loanAccountNumber
, loansdata.disbursementDateTime
,loansdata.clientName
, loansdata.province
, loansdata.dpd
, loansdata.Overdue_Principal
, loansdata.Overdue_Interest
, loansdata.Overdue_Penalty
, loansdata.total_overdue_amount
, loansdata.outstandingBalance
, loansdata.total_outstanding_amount
, cast(coalesce(loansdata.mobileNo, cd.new_mobile_number) as string) primary_number
, case when loansdata.loanAccountNumber in (select loanAccountNumber from segmenta) then 'Segment A'
       when loansdata.loanAccountNumber in (select loanAccountNumber from segmentb) then 'Segment B'
       when loansdata.loanAccountNumber in (select loanAccountNumber from segmentc) then 'Segment C'
       else 'NA' 
 end Segment
, loansdata.ltype Product
, loansdata.lastPaidDT
, rpcdata.lastrpcdate
, lastptp.lastptpdate
, instdata.minunpaidinstallmentnumber
from loansdata
left join instdata on instdata.loanAccountNumber = loansdata.loanAccountNumber
left join rpcdata on rpcdata.loanAccountNumber = loansdata.loanAccountNumber
left join lastptp on lastptp.loanAccountNumber = loansdata.loanAccountNumber
left join cd on  loansdata.customerId = cd.customer_id
)
select * from md 
;

 EXPORT DATA
  OPTIONS(
    uri='gs://prod-tonik-dl-staging-data/report_dumps/loanmasternewformat/outsourceingcriteria16012023_*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=',')
  AS SELECT * from `prj-prod-dataplatform.dap_ds_poweruser_playground.tempdata`; 
