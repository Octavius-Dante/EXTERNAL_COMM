//* MODIFICATION HISTORY
//*============================================================================
//* Changed By.... Goe0042
//* DATE WRITTEN.. 30-Oct-2025
//* R/3 RELEASE... Vendor Spend analysis 
//* DESIGN ID..... For Datasphere
//* CHARM......... 2000013755: EAM-LLM-EPA Information save to Engine
//* Transport No..   SD2K949464
//*============================================================================
//* DESCRIPTION... Vendor Spend analysis 
//*============================================================================
@AbapCatalog.sqlViewName: 'ZEAM_VN_ANA'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #NOT_REQUIRED
@Analytics.dataCategory: #FACT

@EndUserText.label: 'Vendor Spend Analysis'
@Analytics.dataExtraction.enabled: true
//@Analytics.dataExtraction.delta.changeDataCapture.automatic: true
@Analytics.dataExtraction.delta.byElement.name:'Delta_Date'
@Analytics.dataExtraction.delta.byElement.maxDelayInSeconds: 1800
@Analytics.dataExtraction.delta.byElement.detectDeletedRecords: true

define  view  ZEAM_CDS_VENDOR_ANA

as select distinct from  bseg   


inner join  bkpf on bseg.bukrs = bkpf.bukrs and bseg.belnr  = bkpf.belnr and  bseg.gjahr  = bkpf.gjahr 


left outer join  lfa1  on  lfa1.lifnr = bseg.lifnr  
left outer join  lfb1 on  lfb1.lifnr =  bseg.lifnr  and lfb1.bukrs = bseg.bukrs
left outer join  ekpo on ekpo.ebeln = bseg.ebeln and ekpo.ebelp = bseg.ebelp 
left outer join  ekko on ekko.ebeln = bseg.ebeln
left outer join mara on bseg.matnr = mara.matnr

left outer join t001 on t001.bukrs = bseg.bukrs  
left outer join /opt/vim_1head as vn on vn.bukrs =  bseg.bukrs and vn.belnr = bseg.belnr and vn.gjahr = bseg.gjahr
//left outer join  bseg as vn_bg on bseg.bukrs = vn_bg.bukrs and bseg.belnr  = vn_bg.belnr and  bseg.gjahr  = vn_bg.gjahr and  vn_bg.hkont = '0000022000'
//left outer join  bseg as vn_bg1 on bseg.bukrs = vn_bg1.bukrs and bseg.belnr  = vn_bg1.belnr and bseg.gjahr  = vn_bg1.gjahr and vn_bg1.ebeln <> ''
//left outer join  bseg as vn_bg2 on bseg.bukrs = vn_bg2.bukrs and bseg.belnr  = vn_bg2.belnr and bseg.gjahr  = vn_bg2.gjahr and ( vn_bg2.projk <> '00000000' or vn_bg2.kostl <> '' or vn_bg2.aufnr <> '' )

left outer join aufk on bseg.nplnr = aufk.aufnr 
left outer join prps on  prps.objnr = aufk.objnr or bseg.projk = prps.pspnr 



left outer join skat on skat.ktopl = t001.ktopl and bseg.hkont = skat.saknr
//left outer join zmm_ariba_vends as arv on arv.lifnr = bseg.lifnr  
//left outer join t001k as tk on tk.bukrs = bseg.bukrs 
//left outer join mbew on mbew.matnr = bseg.matnr and mbew.bwkey = tk.bwkey  and mbew.bwtar = '' 
{ 

 key bkpf.belnr,   
 key bkpf.gjahr,
 key bkpf.bukrs,
 key bseg.buzei,
bkpf.monat,
bkpf.blart,
bkpf.bldat,
bkpf.budat as PostingDate,
bkpf.cpudt,
bkpf.cputm,
bkpf.tcode,
bkpf.bvorg,
bkpf.xblnr,
bkpf.stblg,
bkpf.bktxt,
bkpf.bstat,
bkpf.glvor,
bkpf.usnam,
bkpf.ppnam,
bkpf.hwaer,
bkpf.hwae2,
bkpf.hwae3,
bseg.augdt,
bseg.augbl,
bseg.bschl,
bseg.koart,
bseg.shkzg,
bseg.ktosl,
bseg.valut,
bseg.zuonr,
bseg.sgtxt,

//COST CENTER
case when bseg.kostl <> '' then bseg.kostl  
      when bseg.aufnr <> '' then  aufk.kostv
     when bseg.projk <> '' then prps.fkstl
   
     else ''
     end as KOSTL,

bseg.hkont,
bseg.lifnr,
bseg.zfbdt,
bseg.zterm,
bseg.meins,
bseg.bustw,
bseg.vertt,
bseg.vertn,
bseg.kstar,
//bseg.ebeln, 
//changed on 21/1/2026
bseg.ebeln,
bseg.ebelp,
bseg.wrbtr,
t001.waers as Local_curr,
currency_conversion( amount => bseg.dmbtr , source_currency => t001.waers, target_currency => cast('USD' as abap.cuky(5)), exchange_rate_date => bkpf.budat ) as DMBTR_USD,
currency_conversion( amount => bseg.dmbtr , source_currency => t001.waers, target_currency => cast('CAD' as abap.cuky(5)), exchange_rate_date => bkpf.budat ) as DMBTR_CAD,
bseg.pswsl,
bseg.pswbt,
bseg.skfbt,
bseg.dmbe2,
bseg.dmbe3,
bseg.wskto,
currency_conversion( amount => bseg.wskto , source_currency => pswsl, target_currency => cast('USD' as abap.cuky(5)), exchange_rate_date => bkpf.budat ) as wskto_USD,
currency_conversion( amount => bseg.wskto , source_currency => pswsl, target_currency => cast('CAD' as abap.cuky(5)), exchange_rate_date => bkpf.budat ) as wskto_CAD,

bseg.zbd1t,
bseg.zbd2t,
bseg.zbd3t,
bseg.zbd1p,
bseg.zbd2p,
bseg.menge,
bseg.aufnr,
bseg.projk,
bseg.nplnr,
bseg.aufpl,
bseg.mwskz,
bseg.txjcd,
ekko.bstyp,
ekko.bsart,
ekko.bsakz,
ekko.loekz,
ekko.statu,
ekko.bedat,
ekko.aedat,
ekko.ernam,
ekko.pincr,
ekko.lponr,

ekko.zterm as PO_ZTERM,
ekko.zbd1t as EKKO_ZBD1T,
ekko.zbd2t as EKKO_ZBD2T,

//EKKO.ZBD1P,
//EKKO.ZBD2P,
ekko.ekgrp,
ekko.waers,
ekko.kdatb,
ekko.kdate,
ekko.inco1,
ekko.inco2,
ekko.lifre,
ekko.frggr,
ekko.frgsx,
ekko.frgke,
ekko.frgzu,
ekko.frgrl,
ekko.ktwrt,
ekko.rlwrt,
ekko.unsez,
ekpo.werks,
ekpo.pstyp,
ekpo.knttp,
ekpo.lgort,
ekpo.infnr,
ekpo.idnlf,
ekpo.netpr,
currency_conversion( amount => ekpo.netpr , source_currency => t001.waers, target_currency => cast('USD' as abap.cuky(5)), exchange_rate_date => bkpf.budat  , error_handling => 'SET_TO_NULL' ) as netpr_USD,
currency_conversion( amount => ekpo.netpr , source_currency => t001.waers, target_currency => cast('CAD' as abap.cuky(5)), exchange_rate_date =>  bkpf.budat   , error_handling => 'SET_TO_NULL' ) as netpr_CAD,
ekpo.peinh,
ekpo.wepos,
ekpo.weunb,
ekpo.repos,
ekpo.webre,
ekpo.konnr,
ekpo.ktpnr,
ekpo.txjcd as EKPO_TXJCD,
ekpo.xersy,
ekpo.banfn,
ekpo.bnfpo,
ekpo.afnam,
ekpo.elikz,
ekpo.packno,
ekpo.erekz,
ekpo.bednr,
ekpo.txz01,
ekpo.matnr,
ekpo.idnlf as EKPO_IDNLF,
ekpo.matkl,
ekpo.bprme,
ekpo.netwr,
ekpo.brtwr,
ekpo.menge as EKPO_MENGE,
lfa1.land1,
lfa1.name1,
lfa1.name2,
lfa1.name3,
lfa1.name4,
lfa1.ort01,
lfa1.ort02,
lfa1.pfach,
lfa1.pstl2,
lfa1.pstlz,
lfa1.regio,
lfa1.stras,
lfa1.adrnr,
lfa1.sortl,
lfa1.brsch,
lfa1.ktokk,
//lfa1.lfurl,
//case when arv.lifnr <> '' then 'Ariba' else '' end as lfurl,
lfb1.lnrze,
lfb1.kverm,
lfb1.mindk,
mara.meins as mara_meins,
mara.ernam as mara_ernam,
mara.ersda,
mara.aenam,
mara.laeda,
mara.mtart,
mara.matkl as MARA_MATKL,
mara.bismt,


////CALCULATIONS:

case when ekko.unsez like  'EP%' then  '' else ekko.unsez end as EKKO_UNSEZ,
 case when ekko.bsart = 'ZAR' then (case when ekko.unsez like  'EP%' then  '' else ekko.unsez end ) else ekpo.konnr end  as ContractNo,
 case when bseg.ebeln  <> ''  then 'TRUE' else 'FALSE' end  as OnPOind,
case
    when ekko.bsart = 'ZAR' then 'Ariba PO'
    when ( bkpf.blart = 'ZY' or bkpf.blart =  'ZZ' ) then 'Direct Feed'
    when  bseg.ebeln  <> ''  then 'SAP PO'
    else 'Other Non-PO'
end as BuyChannel,
//Max eligoble discount
currency_conversion( amount => bseg.skfbt , source_currency => pswsl, target_currency => cast('USD' as abap.cuky(5)), exchange_rate_date =>bkpf.budat  , error_handling => 'SET_TO_NULL' ) as SKFBT_USD,
currency_conversion( amount => bseg.skfbt , source_currency => pswsl, target_currency => cast('CAD' as abap.cuky(5)), exchange_rate_date =>bkpf.budat  , error_handling => 'SET_TO_NULL' ) as SKFBT_CAD,

//case when bseg.belnr <> '' then bseg.dmbe2  else 0 end as SpendOnPO_2,
//case when bseg.belnr <> '' then bseg.dmbe3  else 0 end as SpendOnPO_3,

//( case when bseg.belnr <> '' then BSEG.DMBE2  else 0 end)  as PO_penetration_2,
//(case when bseg.belnr <> '' then BSEG.DMBE3  else 0 end) / BSEG.DMBE2 as PO_penetration_3




//case when left(EKKO.UNSEZ,2) <> 'EP' then EKKO.UNSEZ else '' end as cal_EKKO_UNSEZ

case
   
    when prart = 'C1' or
         prart = 'C2' or
         prart = 'C3' or
         prart = 'C4' or
         prart = 'C6' or
         prart = 'C9' or
         prart = '35' or
         prart = '36' or
         prart = '38' or
         prart = '3A' or
         prart = '3D' 
          then bseg.dmbtr 

    else 0
end as CApital_Spend_local,
case
   
    when prart = 'C1' or
         prart = 'C2' or
         prart = 'C3' or
         prart = 'C4' or
         prart = 'C6' or
         prart = 'C9' or
         prart = '35' or
         prart = '36' or
         prart = '38' or
         prart = '3A' or
         prart = '3D' 
          then t001.waers

    else ''
end as CApital_currency_local,
case
     when 
 prart = '01' or auart = '01' or 
prart = '02' or auart = '02' or 
prart = '03' or auart = '03' or 
prart = '04' or auart = '04' or 
prart = '05' or auart = '05' or 
prart = '06' or auart = '06' or 
prart = '07' or auart = '07' or 
prart = '08' or auart = '08' or 
prart = '09' or auart = '09' or 
prart = '0A' or auart = '0A' or 
prart = '0B' or auart = '0B' or 
prart = '0C' or auart = '0C' or 
prart = '0D' or auart = '0D' or 
prart = '0E' or auart = '0E' or 
prart = '0F' or auart = '0F' or 
prart = '0I' or auart = '0I' or 
prart = '0K' or auart = '0K' or 
prart = '0L' or auart = '0L' or 
prart = '0M' or auart = '0M' or 
prart = '0N' or auart = '0N' or 
prart = '0O' or auart = '0O' or 
prart = '0Q' or auart = '0Q' or 
prart = '0S' or auart = '0S' or 
prart = '0T' or auart = '0T' or 
prart = '10' or auart = '10' or 
prart = '11' or auart = '11' or 
prart = '12' or auart = '12' or 
prart = '13' or auart = '13' or 
prart = '14' or auart = '14' or 
prart = '15' or auart = '15' or 
prart = '16' or auart = '16' or 
prart = '17' or auart = '17' or 
prart = '18' or auart = '18' or 
prart = '19' or auart = '19' or 
prart = '1A' or auart = '1A' or 
prart = '1B' or auart = '1B' or 
prart = '1C' or auart = '1C' or 
prart = '1D' or auart = '1D' or 
prart = '1E' or auart = '1E' or 
prart = '1F' or auart = '1F' or 
prart = '1G' or auart = '1G' or 
prart = '1H' or auart = '1H' or 
prart = '1I' or auart = '1I' or 
prart = '1J' or auart = '1J' or 
prart = '1K' or auart = '1K' or 
prart = '1L' or auart = '1L' or 
prart = '1M' or auart = '1M' or 
prart = '1N' or auart = '1N' or 
prart = '1O' or auart = '1O' or 
prart = '1P' or auart = '1P' or 
prart = '1Q' or auart = '1Q' or 
prart = '1R' or auart = '1R' or 
prart = '1S' or auart = '1S' or 
prart = '1T' or auart = '1T' or 
prart = '1U' or auart = '1U' or 
prart = '1V' or auart = '1V' or 
prart = '1W' or auart = '1W' or 
prart = '1X' or auart = '1X' or 
prart = '1Y' or auart = '1Y' or 
prart = '1Z' or auart = '1Z' or 
prart = '20' or auart = '20' or 
prart = '21' or auart = '21' or 
prart = '22' or auart = '22' or 
prart = '23' or auart = '23' or 
prart = '24' or auart = '24' or 
prart = '25' or auart = '25' or 
prart = '26' or auart = '26' or 
prart = '27' or auart = '27' or 
prart = '28' or auart = '28' or 
prart = '29' or auart = '29' or 
prart = '2A' or auart = '2A' or 
prart = '2B' or auart = '2B' or 
prart = '2C' or auart = '2C' or 
prart = '2D' or auart = '2D' or 
prart = '2E' or auart = '2E' or 
prart = '2F' or auart = '2F' or 
prart = '2G' or auart = '2G' or 
prart = '2H' or auart = '2H' or 
prart = '2I' or auart = '2I' or 
prart = '2J' or auart = '2J' or 
prart = '2K' or auart = '2K' or 
prart = '2L' or auart = '2L' or 
prart = '2M' or auart = '2M' or 
prart = '2N' or auart = '2N' or 
prart = '2O' or auart = '2O' or 
prart = '2P' or auart = '2P' or 
prart = '30' or auart = '30' or 
prart = '31' or auart = '31' or 
prart = '32' or auart = '32' or 
prart = '33' or auart = '33' or 
prart = '34' or auart = '34' or 
prart = '35' or auart = '35' or 
prart = '36' or auart = '36' or 
prart = '37' or auart = '37' or 
prart = '38' or auart = '38' or 
prart = '39' or auart = '39' or 
prart = '3A' or auart = '3A' or 
prart = '3B' or auart = '3B' or 
prart = '3C' or auart = '3C' or 
prart = '3D' or auart = '3D' or 
prart = '3E' or auart = '3E' or 
prart = '3F' or auart = '3F' or 
prart = '3G' or auart = '3G' or 
prart = '3H' or auart = '3H' or 
prart = '3I' or auart = '3I' or 
prart = '3J' or auart = '3J' or 
prart = '3K' or auart = '3K' or 
prart = '3L' or auart = '3L' or 
prart = '3M' or auart = '3M' or 
prart = '3N' or auart = '3N' or 
prart = '3O' or auart = '3O' or 
prart = '3P' or auart = '3P' or 
prart = '3Q' or auart = '3Q' or 
prart = '3R' or auart = '3R' or 
prart = '3S' or auart = '3S' or 
prart = '3T' or auart = '3T' or 
prart = '3U' or auart = '3U' or 
prart = '3V' or auart = '3V' or 
prart = '3W' or auart = '3W' or 
prart = '3X' or auart = '3X' or 
prart = '40' or auart = '40' or 
prart = '41' or auart = '41' or 
prart = '42' or auart = '42' or 
prart = '43' or auart = '43' or 
prart = '44' or auart = '44' or 
prart = '45' or auart = '45' or 
prart = '46' or auart = '46' or 
prart = '47' or auart = '47' or 
prart = '48' or auart = '48' or 
prart = '49' or auart = '49' or 
prart = '4A' or auart = '4A' or 
prart = '4B' or auart = '4B' or 
prart = '4C' or auart = '4C' or 
prart = '4D' or auart = '4D' or 
prart = '4E' or auart = '4E' or 
prart = '4F' or auart = '4F' or 
prart = '4G' or auart = '4G' or 
prart = '4H' or auart = '4H' or 
prart = '50' or auart = '50' or 
prart = '51' or auart = '51' or 
prart = '52' or auart = '52' or 
prart = '53' or auart = '53' or 
prart = '54' or auart = '54' or 
prart = '55' or auart = '55' or 
prart = '56' or auart = '56' or 
prart = '57' or auart = '57' or 
prart = '5A' or auart = '5A' or 
prart = '5B' or auart = '5B' or 
prart = '5C' or auart = '5C' or 
prart = '5D' or auart = '5D' or 
prart = '5E' or auart = '5E' or 
prart = '5F' or auart = '5F' or 
prart = '5G' or auart = '5G' or 
prart = '5J' or auart = '5J' or 
prart = '7A' or auart = '7A' or 
prart = 'C1' or auart = 'C1' or 
prart = 'C2' or auart = 'C2' or 
prart = 'C3' or auart = 'C3' or 
prart = 'C4' or auart = 'C4' or 
prart = 'C5' or auart = 'C5' or 
prart = 'C6' or auart = 'C6' or 
prart = 'C7' or auart = 'C7' or 
prart = 'C8' or auart = 'C8' or 
prart = 'F3' or auart = 'F3' or 
prart = 'M0' or auart = 'M0' or 
prart = 'M1' or auart = 'M1' or 
prart = 'M2' or auart = 'M2' or 
prart = 'M3' or auart = 'M3' or 
prart = 'M4' or auart = 'M4' or 
prart = 'MB' or auart = 'MB' or 
prart = 'MC' or auart = 'MC' or 
prart = 'MD' or auart = 'MD' or 
prart = 'ME' or auart = 'ME' or 
prart = 'MF' or auart = 'MF' or 
prart = 'MG' or auart = 'MG' or 
prart = 'MH' or auart = 'MH' or 
prart = 'MI' or auart = 'MI' or 
prart = 'MY' or auart = 'MY' or 
prart = 'MZ' or auart = 'MZ' or 
prart = 'U1' or auart = 'U1' or 
prart = 'U2' or auart = 'U2' or 
prart = 'U3' or auart = 'U3' or 
prart = 'U4' or auart = 'U4' or 
prart = 'U5' or auart = 'U5' or 
prart = 'U6' or auart = 'U6' or 
prart = 'U7' or auart = 'U7' or 
prart = 'U8' or auart = 'U8' 
     
          then currency_conversion( amount => bseg.dmbtr , source_currency => t001.waers, target_currency => cast('USD' as abap.cuky(5)), exchange_rate_date => bkpf.budat )  

    else 0
end as CApital_Spend_USD,
case when
   
prart = '01' or auart = '01' or 
prart = '02' or auart = '02' or 
prart = '03' or auart = '03' or 
prart = '04' or auart = '04' or 
prart = '05' or auart = '05' or 
prart = '06' or auart = '06' or 
prart = '07' or auart = '07' or 
prart = '08' or auart = '08' or 
prart = '09' or auart = '09' or 
prart = '0A' or auart = '0A' or 
prart = '0B' or auart = '0B' or 
prart = '0C' or auart = '0C' or 
prart = '0D' or auart = '0D' or 
prart = '0E' or auart = '0E' or 
prart = '0F' or auart = '0F' or 
prart = '0I' or auart = '0I' or 
prart = '0K' or auart = '0K' or 
prart = '0L' or auart = '0L' or 
prart = '0M' or auart = '0M' or 
prart = '0N' or auart = '0N' or 
prart = '0O' or auart = '0O' or 
prart = '0Q' or auart = '0Q' or 
prart = '0S' or auart = '0S' or 
prart = '0T' or auart = '0T' or 
prart = '10' or auart = '10' or 
prart = '11' or auart = '11' or 
prart = '12' or auart = '12' or 
prart = '13' or auart = '13' or 
prart = '14' or auart = '14' or 
prart = '15' or auart = '15' or 
prart = '16' or auart = '16' or 
prart = '17' or auart = '17' or 
prart = '18' or auart = '18' or 
prart = '19' or auart = '19' or 
prart = '1A' or auart = '1A' or 
prart = '1B' or auart = '1B' or 
prart = '1C' or auart = '1C' or 
prart = '1D' or auart = '1D' or 
prart = '1E' or auart = '1E' or 
prart = '1F' or auart = '1F' or 
prart = '1G' or auart = '1G' or 
prart = '1H' or auart = '1H' or 
prart = '1I' or auart = '1I' or 
prart = '1J' or auart = '1J' or 
prart = '1K' or auart = '1K' or 
prart = '1L' or auart = '1L' or 
prart = '1M' or auart = '1M' or 
prart = '1N' or auart = '1N' or 
prart = '1O' or auart = '1O' or 
prart = '1P' or auart = '1P' or 
prart = '1Q' or auart = '1Q' or 
prart = '1R' or auart = '1R' or 
prart = '1S' or auart = '1S' or 
prart = '1T' or auart = '1T' or 
prart = '1U' or auart = '1U' or 
prart = '1V' or auart = '1V' or 
prart = '1W' or auart = '1W' or 
prart = '1X' or auart = '1X' or 
prart = '1Y' or auart = '1Y' or 
prart = '1Z' or auart = '1Z' or 
prart = '20' or auart = '20' or 
prart = '21' or auart = '21' or 
prart = '22' or auart = '22' or 
prart = '23' or auart = '23' or 
prart = '24' or auart = '24' or 
prart = '25' or auart = '25' or 
prart = '26' or auart = '26' or 
prart = '27' or auart = '27' or 
prart = '28' or auart = '28' or 
prart = '29' or auart = '29' or 
prart = '2A' or auart = '2A' or 
prart = '2B' or auart = '2B' or 
prart = '2C' or auart = '2C' or 
prart = '2D' or auart = '2D' or 
prart = '2E' or auart = '2E' or 
prart = '2F' or auart = '2F' or 
prart = '2G' or auart = '2G' or 
prart = '2H' or auart = '2H' or 
prart = '2I' or auart = '2I' or 
prart = '2J' or auart = '2J' or 
prart = '2K' or auart = '2K' or 
prart = '2L' or auart = '2L' or 
prart = '2M' or auart = '2M' or 
prart = '2N' or auart = '2N' or 
prart = '2O' or auart = '2O' or 
prart = '2P' or auart = '2P' or 
prart = '30' or auart = '30' or 
prart = '31' or auart = '31' or 
prart = '32' or auart = '32' or 
prart = '33' or auart = '33' or 
prart = '34' or auart = '34' or 
prart = '35' or auart = '35' or 
prart = '36' or auart = '36' or 
prart = '37' or auart = '37' or 
prart = '38' or auart = '38' or 
prart = '39' or auart = '39' or 
prart = '3A' or auart = '3A' or 
prart = '3B' or auart = '3B' or 
prart = '3C' or auart = '3C' or 
prart = '3D' or auart = '3D' or 
prart = '3E' or auart = '3E' or 
prart = '3F' or auart = '3F' or 
prart = '3G' or auart = '3G' or 
prart = '3H' or auart = '3H' or 
prart = '3I' or auart = '3I' or 
prart = '3J' or auart = '3J' or 
prart = '3K' or auart = '3K' or 
prart = '3L' or auart = '3L' or 
prart = '3M' or auart = '3M' or 
prart = '3N' or auart = '3N' or 
prart = '3O' or auart = '3O' or 
prart = '3P' or auart = '3P' or 
prart = '3Q' or auart = '3Q' or 
prart = '3R' or auart = '3R' or 
prart = '3S' or auart = '3S' or 
prart = '3T' or auart = '3T' or 
prart = '3U' or auart = '3U' or 
prart = '3V' or auart = '3V' or 
prart = '3W' or auart = '3W' or 
prart = '3X' or auart = '3X' or 
prart = '40' or auart = '40' or 
prart = '41' or auart = '41' or 
prart = '42' or auart = '42' or 
prart = '43' or auart = '43' or 
prart = '44' or auart = '44' or 
prart = '45' or auart = '45' or 
prart = '46' or auart = '46' or 
prart = '47' or auart = '47' or 
prart = '48' or auart = '48' or 
prart = '49' or auart = '49' or 
prart = '4A' or auart = '4A' or 
prart = '4B' or auart = '4B' or 
prart = '4C' or auart = '4C' or 
prart = '4D' or auart = '4D' or 
prart = '4E' or auart = '4E' or 
prart = '4F' or auart = '4F' or 
prart = '4G' or auart = '4G' or 
prart = '4H' or auart = '4H' or 
prart = '50' or auart = '50' or 
prart = '51' or auart = '51' or 
prart = '52' or auart = '52' or 
prart = '53' or auart = '53' or 
prart = '54' or auart = '54' or 
prart = '55' or auart = '55' or 
prart = '56' or auart = '56' or 
prart = '57' or auart = '57' or 
prart = '5A' or auart = '5A' or 
prart = '5B' or auart = '5B' or 
prart = '5C' or auart = '5C' or 
prart = '5D' or auart = '5D' or 
prart = '5E' or auart = '5E' or 
prart = '5F' or auart = '5F' or 
prart = '5G' or auart = '5G' or 
prart = '5J' or auart = '5J' or 
prart = '7A' or auart = '7A' or 
prart = 'C1' or auart = 'C1' or 
prart = 'C2' or auart = 'C2' or 
prart = 'C3' or auart = 'C3' or 
prart = 'C4' or auart = 'C4' or 
prart = 'C5' or auart = 'C5' or 
prart = 'C6' or auart = 'C6' or 
prart = 'C7' or auart = 'C7' or 
prart = 'C8' or auart = 'C8' or 
prart = 'F3' or auart = 'F3' or 
prart = 'M0' or auart = 'M0' or 
prart = 'M1' or auart = 'M1' or 
prart = 'M2' or auart = 'M2' or 
prart = 'M3' or auart = 'M3' or 
prart = 'M4' or auart = 'M4' or 
prart = 'MB' or auart = 'MB' or 
prart = 'MC' or auart = 'MC' or 
prart = 'MD' or auart = 'MD' or 
prart = 'ME' or auart = 'ME' or 
prart = 'MF' or auart = 'MF' or 
prart = 'MG' or auart = 'MG' or 
prart = 'MH' or auart = 'MH' or 
prart = 'MI' or auart = 'MI' or 
prart = 'MY' or auart = 'MY' or 
prart = 'MZ' or auart = 'MZ' or 
prart = 'U1' or auart = 'U1' or 
prart = 'U2' or auart = 'U2' or 
prart = 'U3' or auart = 'U3' or 
prart = 'U4' or auart = 'U4' or 
prart = 'U5' or auart = 'U5' or 
prart = 'U6' or auart = 'U6' or 
prart = 'U7' or auart = 'U7' or 
prart = 'U8' or auart = 'U8' 

   then currency_conversion( amount => bseg.dmbtr , source_currency => t001.waers, target_currency => cast('CAD' as abap.cuky(5)), exchange_rate_date => bkpf.budat )  

    else 0
end as CApital_Spend_CAD,
prps.prart,
//Add approver
case when ekpo.afnam <> '' then ekpo.afnam else vn.requisitioner   end as requisitioner,
 //VENDOR
 bseg.lifnr  as Vendor,
skat.txt50 as GL_TEXT,
//arv.trading_rel_accepted_date ,
//mbew.salk3 ,//inventory value

case when bseg.hkont = '0000022000' then 1 else 0 end as CNT_INV,
t001.butxt,
 //WBS element
 prps.poski ,
case when bkpf.aedat>= cpudt then bkpf.aedat else cpudt end as Delta_Date

} 
where
 bkpf.budat >= '20240101'
 and
( bkpf.blart = 'XE'
or bkpf.blart = 'XF'
or bkpf.blart = 'ZA'
or bkpf.blart = 'ZB'
or bkpf.blart = 'ZD'
or bkpf.blart = 'ZE'
or bkpf.blart = 'ZF'
or bkpf.blart = 'ZX'
or bkpf.blart = 'ZY'
or bkpf.blart = 'ZZ'
or bkpf.blart = 'KA'
or bkpf.blart = 'XI'
or bkpf.blart = 'XL'
or bkpf.blart = 'ZL' ) 

;

// and


 
//bkpf.budat >= cast( substring( cast( tstmp_current_utctimestamp() as abap.char( 17 ) ),1,8) as abap.dats) ;

