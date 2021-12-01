program define read_lz_full_data
tempfile LZData self father gfather ggfather
import excel using "${other_data}LZ Data for Jinshenlu linkage.xlsx", firstrow locale("Chinese") allstring
keep xing ming ren_sheng ren_xian GGFming GFming Fming LZ_id
save "`LZData'", replace
keep xing ming ren_sheng ren_xian LZ_id
generate relationship = 1
save "`self'", replace
use "`LZData'"
keep xing Fming ren_sheng ren_xian LZ_id
rename Fming ming
generate relationship = 2
save "`father'", replace
use "`LZData'"
keep xing GFming ren_sheng ren_xian LZ_id
rename GFming ming
generate relationship = 3
save "`gfather'", replace
use "`LZData'"
keep xing GGFming ren_sheng ren_xian LZ_id
rename GGFming ming
generate relationship = 4
save "`ggfather'", replace
drop _all
use "`self'"
append using "`father'"
append using "`gfather'"
append using "`ggfather'"
fix_characters
save "${work_files}LZ full data", replace
drop _all
end

* Here is the program that reads in the yudie .xlsx files
program define read_yudie
import excel using "${other_data}zongshi_complete_zp_2010_8_23.xlsx", firstrow locale("Chinese") allstring
keep name bemperor byear bmonth bday demperor dyear dmonth dday
fix_characters
rename name ming
destring bemperor-dday, replace force
save "${work_files}Zongshi", replace
drop _all

import excel using "${other_data}axjl_zongpu_jueluo_2011_07 pinyin headers.xlsx", firstrow locale("Chinese") allstring
keep name bemperor byear bmonth bday demperor dyear dmonth dday
fix_characters
rename name ming
destring bemperor-dday, replace force
save "${work_files}Jueluo", replace
append using "${work_files}Zongshi"
save "${work_files}Zongshi and jueluo", replace
drop _all
end

program define read_henan_juren
import excel using "${other_data}清代河南舉人數據信息 cdc.xlsx", firstrow locale("Chinese") allstring
keep 朝代-備註
fix_characters
rename 籍貫 ren_xian
generate ren_sheng = "河南"
replace ren_xian = usubinstr(ren_xian,"縣","",.) if ustrlen(ren_xian) > 2
rename 姓 xing
rename 名 ming
destring 公曆年 中式年齡 中式名次, replace force
generate birthyear = 公曆年 - 中式年齡 + 1
save "${other_data}清代河南舉人數據信息 cdc", replace
drop _all
end

program define read_zhujuan

import excel using "${cbdb_directory}CBDB Zhujuan from Hongsu 1.xlsx", firstrow locale("Chinese") allstring
fix_characters
save "${cbdb_directory}CBDB Zhujuan 1", replace
keep if 頁碼 != ""

*use "${cbdb_directory}CBDB Zhujuan 1"

keep 姓名 地址
replace 姓名 = subinstr(姓名," ","",.) 
save "${cbdb_directory}CBDB Zhujuan 1 degree holder names", replace
drop _all 

import excel using "${cbdb_directory}CBDB Zhujuan from Hongsu 2.xlsx", firstrow locale("Chinese") allstring
fix_characters
save "${cbdb_directory}CBDB Zhujuan 2", replace
keep if 頁碼 != ""

*use "${cbdb_directory}CBDB Zhujuan 2"

keep 姓名 信息
replace 姓名 = subinstr(姓名," ","",.) 
save "${cbdb_directory}CBDB Zhujuan 2 degree holder names", replace

append using "${cbdb_directory}CBDB Zhujuan 1 degree holder names"

save "${cbdb_directory}CBDB Zhujuan all degree holder names", replace

end

program define read_timinglu_from_yuxue
import excel using "${other_data}清代进士题名录繁體20160319 from Yuxue.xlsx", firstrow locale("Chinese") allstring
fix_characters
rename 省 ren_sheng
rename 縣或州 ren_xian
replace ren_sheng = usubinstr(ren_sheng,"省","",.)
replace ren_xian = usubinstr(ren_xian,"縣","",.)
replace ren_xian = usubinstr(ren_xian,"州","",.)

save "${work_files}Timinglu from Yuxue", replace
end

program define read_cbdb 

import excel using "${cbdb_directory}ZZZ_BIOG_MAIN.xlsx", firstrow locale("Chinese") allstring
keep c_personid c_name c_name_chn c_female c_birthyear c_deathyear c_death_age c_surname c_surname_chn c_mingzi c_mingzi_chn c_ethnicity_chn c_ethnicity_rmn
fix_characters
tab c_female
*keep if c_female == "FALSE"
destring c_personid, replace
rename c_mingzi_chn ming 
rename c_surname_chn xing
destring c_birthyear c_deathyear c_death_age, replace 
save "${cbdb_directory}CBDB biography", replace
clear

import excel using "${cbdb_directory}ZZZ_ENTRY_DATA.xlsx", firstrow locale("Chinese") allstring
keep c_personid c_entry_code c_entry_desc_chn c_exam_rank c_year c_nianhao_chn c_entry_nh_year c_notes c_addr_desc_chn c_addr_chn c_name_chn c_index_year
fix_characters
destring c_personid, replace
generate chushen_1 = "進士" if ustrpos(c_entry_desc_chn,"進士") > 0
replace chushen_1 = "舉人" if ustrpos(c_entry_desc_chn,"舉人") > 0
replace chushen_1 = "生員" if ustrpos(c_entry_desc_chn,"生員") > 0
replace chushen_1 = "封贈" if ustrpos(c_entry_desc_chn,"封贈") > 0
destring c_year c_index_year c_entry_nh_year, replace
save "${cbdb_directory}CBDB entry", replace
clear

end

program define read_guanzhi_pinji
display "Entering: read_guanzhi_pinji"
log using "${log_directory}Jin shen lu read guanzhi pinji $time_stamp", text

clear
set more off

import excel using "${pinji_recodes}pinji numeric.xlsx", firstrow locale("Chinese") allstring
*set trace on
keep guanzhi_pinji pinji_numeric
fix_characters
destring pinji_numeric, replace
bysort guanzhi_pinji: keep if _n == 1
save "${work_files}pinji numeric", replace
clear

import excel using "${pinji_recodes}Core guanzhi manually assigned pinji.xlsx", firstrow locale("Chinese") allstring
keep core_guanzhi guanzhi_pinji pinji_1_3 pinji_4_6 pinji_7_9 pinji_none not_guanzhi
fix_characters
bysort core_guanzhi:keep if _n == 1
destring pinji_1_3 pinji_4_6 pinji_7_9 pinji_none not_guanzhi, replace
mvencode pinji_1_3 pinji_4_6 pinji_7_9 pinji_none not_guanzhi, mv(0)

generate guanzhi_pinji_category = ""
replace guanzhi_pinji_category = "1-3" if pinji_1_3
replace guanzhi_pinji_category = "4-6" if pinji_4_6
replace guanzhi_pinji_category = "7-9" if pinji_7_9
drop pinji_1_3 pinji_4_6 pinji_7_9
drop if core_guanzhi == ""
*drop Note
save "${pinji_recodes}Core guanzhi manually assigned pinji", replace
clear

import excel using "${pinji_recodes}Guanzhi diqu jigou manually assigned pinji.xlsx", firstrow locale("Chinese") allstring
keep guanzhi diqu jigou_1 jigou_2 jigou_3 guanzhi_pinji guanzhi_pinji_category
fix_characters
bysort guanzhi diqu jigou_1 jigou_2 jigou_3: keep if _n == 1
keep if guanzhi_pinji_category != "" | guanzhi_pinji != ""

replace diqu = "" if diqu == "空白"
replace jigou_1 = "" if jigou_1 == "空白"
replace jigou_2 = "" if jigou_2 == "空白"
replace jigou_3 = "" if jigou_3 == "空白"

*keep if guanzhi != ""
bysort guanzhi diqu jigou_1 jigou_2 jigou_3: keep if _n == 1
save "${pinji_recodes}Guanzhi diqu jigou manually assigned pinji", replace
clear

import excel using "${pinji_recodes}Guanzhi manually assigned pinji.xlsx",firstrow locale("Chinese") allstring
keep guanzhi guanzhi_pinji guanzhi_pinji_category 

*set trace on
*set tracedepth 2
fix_characters
*set trace off

* For the time being, if there are duplicates, only keep one of them

bysort guanzhi: keep if _n == 1

compress

save "${pinji_recodes}Guanzhi manually assigned pinji", replace

drop _all
log close
display "Exiting: read_guanzhi_pinji“
end

program define read_other
import excel using "${conversion}ganzhi.xlsx",locale("Chinese") allstring firstrow
destring ganzhi_year, replace
save "${conversion}ganzhi", replace
drop _all
end

