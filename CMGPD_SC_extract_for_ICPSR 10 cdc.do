
* Modified February 2, 2014 to clean up issues discovered in beta release, including location of UNIQUE_VILLAGE_ID, handling of toupin etc.
* Modified May 3, 2014 to fix the ethnicity recodes

clear 
* Creates version of the extract for ICPSR
*! Adapted from Cameron's "extract_for_ICPSR 7" last modified on 8/23/2012
* data_directory refers to directory containing 'frozen' version to ensure all extracts are consistent
 *local cdo C:\Users\hbwang\Dropbox\Acad\Projects\CMGPD\Analysis\Code\Data\combined ICPSR master
 local cdo "C:\Users\camcam\Dropbox\Documents\CMGPD\programs"
 local data_directory "combined"
 *local WD "D:\HBW\Acad\Projects\CMGPD-SC\WDAT"
 cd "`cdo'"
 
 cap program drop write_files
 program define write_files 

 local extract_directory "CMGPD-SC Extracts"
 outsheet using "../`extract_directory'/SC for ICPSR `1' `2'.txt", replace nolabel 
 save "../`extract_directory'/SC for ICPSR `1' `2'", replace
 saveold "../`extract_directory'/SC for ICPSR `1' `2' v 10", replace
 end

*! Accordingly, all "../`data_directory'" below are changed to "`data_directory'"
set more off

tempfile liaoning scp liaoning_digital father_name gf_name mother_name wife wife_name hh_head_name husband alive_wives alive_husbands married_moms widowed_moms name_file

use "../`data_directory'/household_registers_raw" if dataset >=101
keep dataset case date interpol recno
merge 1:1 dataset case date interpol using "../`data_directory'/Household register names and notes", keep(match master) keepusing(xing ming hxing hming fxing fming gfming)
drop case date interpol
sort dataset recno
generate record_number = string(_n, "%09.0f")
drop recno _merge
save "`name_file'"

use id date kin_md_f_1_ma kin_hh_md_f_1_ma using "../`data_directory'/household_registers_kin_counts_married" 
sort date id
by date id: keep if _n == 1
sort date id
save "`married_moms'"

use id date kin_wid_f_1_ma kin_hh_wid_f_1_ma using "../`data_directory'/household_registers_kin_counts_widowed" 
sort date id
by date id: keep if _n == 1
sort date id
save "`widowed_moms'"

use id date present sex using "../`data_directory'/household_registers_generated" if sex == 1 & present
sort id date
by id date: keep if _n == 1
rename id w_id
sort w_id date
drop sex
save "`alive_wives'"

use id date present sex using "../`data_directory'/household_registers_generated" if sex == 2 & present
sort id date
by id date: keep if _n == 1
rename id h_id
sort h_id date
save "`alive_husbands'"

use "Hukouce"
sort dataset
save, replace

* No idea why the following code was here.

/*

use "../`data_directory'/household_registers_raw" if dataset >=101
keep id date name
sort id date
by id: keep if _n == _N
rename id f_id_1 
rename name f_name
drop date
sort f_id_1 
save "`father_name'"

rename f_id_1 f_id_2
sort f_id_2
rename f_name gf_name
save "`gf_name'"

replace gf_name = subinstr(gf_name," shi","",.)

rename f_id_2 m_id_1
sort m_id_1
rename gf_name m_name
save "`mother_name'"

rename m_id_1 w_id
sort w_id
rename m_name wife_name
save "`wife_name'"

*/


use h_id w_id date marriage_first dataset using "../`data_directory'/household_registers_marriages" if dataset >=101

sort h_id date w_id
by h_id date w_id: keep if _n == 1

desc, short

tempvar repeats
bysort w_id date: generate `repeats' = _N 
tab `repeats'
drop `repeats'

sort w_id date
merge w_id date using "`alive_wives'", nokeep
keep if present != .
drop present _merge

display "Should be no change in the number of records"
desc, short

display "Records should still be unique"

sort h_id date marriage_first w_id
by h_id date: generate j = _n 
drop marriage_first dataset

reshape wide w_id, i(h_id date) j(j)

rename h_id id

generate year = date+1000
drop date

display "Repeated husbands"

sort id year

save "`wife'"

use h_id w_id date marriage_first dataset using "../`data_directory'/household_registers_marriages" if dataset >=101

sort w_id date h_id
by w_id date h_id: keep if _n == 1

sort h_id date
merge h_id date using "`alive_husbands'", nokeep
keep if present != .
drop present _merge

sort w_id h_id 
by w_id h_id: generate h_obs = _N

sort w_id date h_obs
by w_id date: keep if _n ==  _N

drop marriage_first dataset h_obs

rename w_id id

generate year = date+1000
drop date

sort id year

save "`husband'"

use dataset date case interpol id age clan hid recno vital1 vital2 marital gen rhhead sex name occu sangfu_year disabil birthyr month day hour nation zuoling_qi zuoling_diming zuoling_ming qianzhudi zhenghudizhi using "../`data_directory'/household_registers_raw" if dataset >=101

sort dataset recno
generate record_number = string(_n, "%09.0f")

desc, short

desc, short 
sort date id
merge date id using "`married_moms'", nokeep
drop _merge

sort date id
merge date id using "`widowed_moms'", nokeep
drop _merge

* The mother_alive had to be constructed using different variables based on presence of maternal (*_ma) kin

generate byte mother_alive = kin_wid_f_1_ma | kin_md_f_1_ma
generate byte kin_f_1_ma = mother_alive
generate byte kin_hh_f_1_ma = kin_hh_wid_f_1_ma | kin_hh_md_f_1_ma

desc, short

generate int year = date+1000

rename clan zu_id
rename hid hh_id

rename gen generation
replace generation = 0 -99 if generation == 0 | generation == -1

rename birthyr bazi_year
rename month bazi_month
rename day bazi_day
rename hour bazi_hour

replace bazi_year = -99 if bazi_year == . | bazi_year <= 0 | bazi_year > 12
replace bazi_month = -99 if bazi_month == . | bazi_month <= 0 | bazi_month > 12
replace bazi_day = -99 if bazi_day == . | bazi_day <= 0 | bazi_day > 30
replace bazi_hour = -99 if bazi_hour == . | bazi_hour <= 0 | bazi_hour > 12

sort id 
merge id using "../`data_directory'/household_registers_ancestors", keep(f_id_1 f_id_2 f_id_3 f_id_4 m_id_1 m_id_2 m_id_3 m_id_4) nokeep
drop _merge

generate birthyear = year + 1 - age

desc, short

sort id year
merge id year using "`wife'", nokeep
drop _merge

desc, short

sort id year
merge id year using "`husband'", nokeep
drop _merge

compress

local kin_merge ""

/*
foreach x in varlist kin_m_1-kin_m_1_pa kin_hh_m_1-kin_hh_m_1_pa {
	local kinmerge "`kin_merge' `x'"
}
*/

desc, short
merge dataset recno using "../`data_directory'/household_registers_kin_counts_all.dta", sort nokeep 
drop _merge

merge id date recno using "../`data_directory'/household_registers_kin_counts_widowed.dta", sort nokeep
drop _merge

merge id date recno using "../`data_directory'/household_registers_kin_counts_married.dta", sort nokeep
drop _merge

desc, short

/*
sort dataset recno
by dataset recno: drop if _n > 1
*/

generate byte brother_count = kin_hh_m_1 
generate byte sister_count = kin_hh_f_1
generate byte male_cousin_count = kin_hh_m_2
generate byte female_cousin_count = kin_hh_f_2
generate byte uncle_count = kin_hh_m_2_pa
generate byte aunt_count = kin_hh_f_2_pa
generate byte father_alive = kin_hh_m_1_pa

merge dataset recno using "../`data_directory'/household_registers_generated.dta", sort nokeep keep(unique_village address present surname_yihu at_risk_die at_risk_marry at_risk_remarry next_die next_tao next_boys next_girls boys girls next_marry next_remarry present next1 next3 next6 position tao tui lao area location artisan soldier unique_village valid_village zu_zhang occu bai_zong qian_zong xiao_qi_xiao bi_tie_shi ling_cui zhi_shi_ren service has_surname non_han_name diminutive rustic number pin dingdai juan hou_bu wei_guan man_zhou income honorif man_zhou new purchased tou_cong expelled gao_li bao_yang birth_order birth_order_sex seniority seniority_sex ever_tao exam gao_li founder_ancestor founder_inferred gu hh_divide_next hh_size popstatus qi unique_group unique_hh_id unique_yihu)

drop _merge

generate mother_id = m_id_1
generate father_id = f_id_1
generate grandfather_id = f_id_2

rename id person_id
rename w_id1 wife_1_id
rename w_id2 wife_2_id
rename next1 next_1
rename next3 next_3
rename next6 next_6
rename tao absconded
rename tui retired
rename lao old
rename boys son_count
rename girls daughter_count
rename next_tao next_absconded

* Renames for analytic release

rename bao_yang adopted
rename diminutive diminutive_name
rename dingdai ding_dai
rename disabil disability_code
rename ever_tao ever_absconded
rename exam examination
rename founder_ancestor founder_id
rename founder_inferred founder_inferred_id
rename gu dead
rename honorif honorific
rename income estimated_income
rename new new_ding
rename number number_name
rename occu position_code
rename pin rank
rename popstatus population_category
rename purchased purchased_title
rename qi banner
rename rustic rustic_name
rename service service_ding
rename unique_yihu unique_yi_hu

replace unique_village = . if !valid_village
drop valid_village

rename h_id husband_id

sort dataset year case interpol

rename rhhead relationship

generate household_id = string(dataset,"%03.0f")+string(date+1000,"%04.0f")+string(zu_id,"%03.0f")+string(hh_id,"%02.0f")
*generate record_id = string(dataset,"%03.0f")+string(recno,"%06.0f")
generate register_seq = string(case,"%05.0f")+string(interpol,"%02.0f")
generate record_id = string(dataset,"%03.0f")+string(year,"%04.0f")+register_seq

mvencode founder_inferred_id father_id grandfather_id husband_id wife_1_id wife_2_id founder_id unique_group unique_yi_hu unique_hh_id, mv(-99) override 

sort person_id
generate max_id = person_id[_N]

generate byte father_id_imputed = father_id > max_id
generate byte grandfather_id_imputed = grandfather_id > max_id

drop max_id

* Kinship linking variables
* Fixed August 6, 2011

tostring founder_id, replace format("%08.0f")
tostring person_id, replace format("%08.0f")
tostring mother_id, replace format("%08.0f")
tostring father_id, replace format("%08.0f")
tostring grandfather_id, replace format("%08.0f")
tostring husband_id, replace format("%08.0f")
tostring wife_1_id, replace format("%08.0f")
tostring wife_2_id, replace format("%08.0f")
tostring f_id_* m_id_*, replace format("%08.0f")

* Grouping variables

tostring founder_inferred_id, replace format("%010.0f")
tostring unique_group, replace format("%010.0f")
tostring unique_yi_hu, replace format("%010.0f")
tostring unique_hh_id, replace format("%011.0f")

* Want the strings for missing to be -99 instead of -000000099 or whatever

foreach x of varlist f_id_1 f_id_2 f_id_3 f_id_4 m_id_1 m_id_2 m_id_3 m_id_4 founder_inferred_id father_id grandfather_id husband_id wife_1_id wife_2_id founder_id unique_group unique_yi_hu unique_hh_id {
	replace `x' = "-99" if real(`x') == -99 | real(`x') == 0
	replace `x' = "-98" if real(`x') == -98
}

* Force spouse IDs to structural missing for people who are widowed or single

replace wife_1_id = "-98" if sex == 1 | marital == 2 | marital == 3
replace wife_2_id = "-98" if sex == 1 | marital == 2 | marital == 3
replace husband_id = "-98" if sex == 2 | marital == 2 | marital == 3

* Force birth order, seniority variables to missing for individuals without a father ID
* Added 4/12/2011

foreach x of varlist birth_order birth_order_sex seniority seniority_sex brother_count sister_count father_alive {
	replace `x' = -98 if real(father_id) == -99
}

* Force household size, birth order, senior variables to missing where their computed value is zero. This is mainly married/widowed women, and a small number of men.
* added 4/24/2014

foreach x of varlist hh_size birth_order birth_order_sex seniority seniority_sex {
	replace `x' = -98 if `x' == 0
}

replace mother_alive = -98 if real(mother_id) == -99

* Force all kinship counts to structural missing when relevant common ancestor isn't identified 

foreach x of varlist kin*1 {
	replace `x' = -98 if real(f_id_1) == -99 | real(f_id_1) == -98
}

foreach x of varlist kin*2 kin*2_pa {
	replace `x' = -98 if real(f_id_2) == -99 | real(f_id_2) == -98
}

foreach x of varlist kin*3 kin*3_pa {
	replace `x' = -98 if real(f_id_3) == -99 | real(f_id_3) == -98
}

foreach x of varlist kin*4 kin*4_pa {
	replace `x' = -98 if real(f_id_4) == -99 | real(f_id_4) == -98
}

foreach x of varlist kin_f_1_ma kin_hh_f_1_ma kin_hh_md_f_1_ma kin_wid_f_1_ma kin_hh_wid_m_1_pa {
	replace `x' = -98 if real(m_id_1) == -99 | real(m_id_1) == -98
	}
	
foreach x of varlist kin_m_1_pa kin_hh_m_1_pa kin_hh_md_m_1_pa kin_wid_m_1_pa kin_hh_wid_m_1_pa {
	replace `x' = -98 if real(f_id_1) == -99 | real(f_id_1) == -98
	}

replace marital = 4 if marital > 4
rename marital marital_status
replace marital_status = -99 if marital_status == 0

* There are no zero ages in the original data, if an age is zero, it means it was missing in the original data.  If it is less than zero, 
* some other kind of problem, probably age originally missing and at some point we made a failed effort at machine assignment of age.

replace age = -99 if age <= 0

local date_time "$S_DATE $S_TIME"
local date_time = subinstr("`date_time'",":","",.)

rename hh_id household_seq
rename zu_id yihu_seq

drop recno
*rename recno record_seq

*label variable record_seq "Record sequence number, within dataset"
label variable yihu_seq "Yihu sequence number, within register"
label variable household_seq "Household sequence number, within yihu"

*label variable record_id "Unique record identifier, concatenation of dataset and record_seq"

generate died = vital1 == 1 | vital2 == 1
generate married_out = vital1 == 2 | vital2 == 2
generate remarried_out = vital1 == 3 | vital2 == 3
generate adopted_in = vital1 == 8 | vital2 == 8
generate adopted_out = vital1 == 11 | vital2 == 11

rename area prefecture
rename location region

drop date case interpol
rename age age_in_sui
rename vital1 event_1
rename vital2 event_2

replace event_1 = -99 if event_1 == 0 | event_1 > 25
replace event_2 = -99 if event_2 == 0 | event_2 > 25

generate byte no_status = (position_code == 0 | position_code == -1 | position_code == .)
replace no_status = -98 if sex != 2

replace household_seq = -98 if year < 1789
replace hh_size = -98 if year < 1789
*replace unique_hh_id = "-98" if year < 1789
replace hh_divide_next = -98 if year < 1789
* If banner is 0, it means that there was a village recorded, but no banner recorded alongside the village
replace banner = -99 if banner == 0
* If banner is system missing, it means that there wasn't any address recorded at all
replace banner = -98 if banner == .

sort record_id

order record_number record_id person_id surname_yihu name mother_id father_id father_id_imputed grandfather_id grandfather_id_imputed wife_1_id wife_2_id husband_id f_id_1-f_id_4 m_id_1-m_id_4 household_id unique_hh_id unique_group unique_yi_hu founder_inferred_id founder_id dataset year register_seq yihu_seq household_seq region prefecture banner population_category unique_village address relationship sex generation marital_status age_in_sui birthyear brother_count sister_count male_cousin_count female_cousin_count uncle_count aunt_count father_alive mother_alive son_count daughter_count present died married_out remarried_out next_die next_marry next_remarry next_absconded next_boys next_girls next_3 next_6 at_risk_die at_risk_marry at_risk_remarry birth_order birth_order_sex seniority seniority_sex hh_size hh_divide_next position_code absconded ever_absconded expelled tou_cong adopted dead no_status retired old new_ding service_ding disability_code position rank ding_dai zu_zhang artisan soldier bai_zong qian_zong xiao_qi_xiao bi_tie_shi zhi_shi_ren ling_cui honorific examination juan purchased_title has_surname non_han_name diminutive_name rustic_name number_name man_zhou gao_li kin_m_1-kin_m_1_pa kin_f_1_ma kin_hh_m_1-kin_hh_m_1_pa kin_hh_f_1_ma kin_md_m_1-kin_md_m_1_pa kin_md_f_1_ma kin_hh_md_m_1-kin_hh_md_m_1_pa kin_hh_md_f_1_ma kin_wid_m_1-kin_wid_m_1_pa kin_wid_f_1_ma kin_hh_wid_m_1-kin_hh_wid_m_1_pa kin_hh_wid_f_1_ma bazi_year bazi_month bazi_day bazi_hour

mvencode unique_village age_in_sui birthyear prefecture, mv(-99) override

replace birthyear = -98 if age_in_sui == -99

* Force constructed counts, flags etc. for people who are not present (dead, married out etc.) to structural missing, -98

foreach x of varlist brother_count-daughter_count next_die-at_risk_remarry {
	replace `x' = -98 if !present
} 

* Force position/title/status variables to structural missing for women
* Leaving position_code as in original since sometimes it is non-zero for women, but changing 0 and -1 to -99

replace position_code = -99 if position_code == 0 | position_code == -1

replace disability_code = -99 if disability_code == 0

foreach x of varlist absconded-gao_li estimated_income {
	replace `x' = -98 if sex != 2
}

foreach x of varlist kin_hh_m_1-kin_hh_f_1_ma {
	replace `x' = -98 if year < 1789
}

* Force variables recording occurrance of events in next register to structural missing in the last register in each series

bysort dataset (year): generate byte last_register = year == year[_N]

foreach x of varlist hh_divide_next next_die next_marry next_remarry next_absconded next_boys next_girls {
	replace `x' = -98 if last_register
}

replace sex = -99 if sex == 0

label define marital 1 "Married" 2 "Unmarried" 3 "Widowed" 4 "Remarried"
label values marital_status marital

label define sex1 1 "Female" 2 "Male"
label values sex sex1 

compress

*+++
replace mother_id="-99" if mother_id=="00000000"

*+++

write_files "`date_time'" full_raw

preserve

keep dataset year relationship generation sex age_in_sui birthyear nation marital_status /*father_name grandfather_name father_position grandfather_position */ ///
     event_1 event_2 adopted_in adopted_out married_out remarried_out absconded died present  ///
	 position_code disability_code no_status retired position soldier rank estimated_income examination /*guan_xue_sheng*/ honorific juan purchased_title hou_bu dead new_ding sangfu_year  ///
	 father_alive mother_alive son_count daughter_count brother_count sister_count male_cousin_count female_cousin_count uncle_count aunt_count ///
	 at_risk_die at_risk_marry at_risk_remarry next_1 next_3 next_boys next_girls next_die next_marry next_remarry next_absconded ///
	 birth_order birth_order_sex hh_size hh_divide_next  ///
	 diminutive_name has_surname non_han_name number_name rustic_name zuoling_qi zuoling_diming zhenghudizhi qianzhudi ///
	 record_number register_seq household_seq yihu_seq household_id ///	 
     person_id father_id mother_id father_id_imputed grandfather_id grandfather_id_imputed wife_1_id wife_2_id husband_id founder_id founder_inferred_id unique_group unique_hh_id unique_yi_hu address  
 
order dataset year relationship generation sex age_in_sui birthyear nation marital_status /*father_name grandfather_name father_position grandfather_position */ ///
     event_1 event_2 adopted_in adopted_out married_out remarried_out absconded died present  ///
	 position_code disability_code no_status retired position soldier rank estimated_income examination /*guan_xue_sheng*/ honorific juan purchased_title hou_bu dead new_ding sangfu_year  ///
	 father_alive mother_alive son_count daughter_count brother_count sister_count male_cousin_count female_cousin_count uncle_count aunt_count ///
	 at_risk_die at_risk_marry at_risk_remarry next_1 next_3 next_boys next_girls next_die next_marry next_remarry next_absconded ///
	 birth_order birth_order_sex hh_size hh_divide_next  ///
	 diminutive_name has_surname non_han_name number_name rustic_name   ///
	 record_number register_seq household_seq yihu_seq  zuoling_qi zuoling_diming qianzhudi ///	 
     person_id father_id mother_id father_id_imputed grandfather_id grandfather_id_imputed wife_1_id wife_2_id husband_id founder_id founder_inferred_id unique_group household_id unique_hh_id unique_yi_hu   

rename address raw_village_id
*rename unique_village unique_village_id
	 
*Unique village ID
*According to Chen Shuang
label def L_banner 101	"SC:Zhenghuang" 102	"SC:Xianghuang" 103	"SC:Zhengbai" 104	"SC:Xiangbai" 105 "SC:Zhenghong" 106 "SC:Xianghong" 107	"SC:Zhenglan" 108 "SC:Xianglan"

* Eqi isn't in the User Guide, so I don't know why we have it here.

/*
gen eqi:L_banner=dataset
  label var eqi "8-Banner population"
  replace eqi=dataset-8 if (dataset>=109 & dataset<=116) /*& (address>=1 & address<=20)*/
*/

tempvar sdata vil
gen `sdata'=dataset
  replace `sdata'=105 if dataset==101  /*incorporate 101 "zhenghuang-jingqi" into 105 "zhenghong-tunding" */
  replace `sdata'=103 if dataset==102  /*incorporate 102 "zhenghuang-jingqi" into 103 "zhengbai-tunding" */
 
  replace `sdata'=dataset-8 if (dataset>=109 & dataset<=116) & (raw_village_id >=1 & raw_village_id <=20) /*keep wo peng*/

 gen `vil': L_tun=raw_village_id
  replace `vil'=-99 if raw_village_id==0  


 gen        unique_village_number=`sdata'*100+`vil'
  label var unique_village_number "Village(tun) ID (based on dataset and address)"

 egen       unique_village_id=group(unique_village_number)  /* gen won't work */
  label var unique_village_id "Unique Village(tun) ID (based on dataset and address, equvi. tun_id)"

replace unique_village_id = -99 if raw_village_id == 0
drop unique_village_number

drop raw_village_id
drop `sdata' `vil'

replace rank = 1 if rank == 10
replace rank = -99 if rank == 0

generate ethnicity = 0	
replace ethnicity = nation
replace ethnicity = -99 if nation==0
 
replace ethnicity=2	if dataset==101	& nation==1
replace ethnicity=3	if dataset==101	& nation==2
replace ethnicity=4	if dataset==101	& nation==3

replace ethnicity=4	if dataset==102	& nation==1

replace ethnicity=4	if dataset==104	& nation==3
replace ethnicity=5	if dataset==104	& nation==4
replace ethnicity=3	if dataset==104	& nation==5

replace ethnicity=2	if dataset==106	& nation==1
replace ethnicity=3	if dataset==106	& nation==2
replace ethnicity=4	if dataset==106	& nation==3
replace ethnicity=1	if dataset==106	& nation==4

 
label def ethnic 1 "Han" 2 "Manchu" 3 "Xibo" 4	"Mongol" 5	"Taimanzi" 6 "Baerhu" 7	"Xibo-Machu"
label val ethnicity ethnic 

drop nation
 
label def L_population_category 1 "Jingqi (High)" 2 "Tunding (middle)" 3 "Fuding (low)"
gen        population_category:L_population_category=1 if dataset==101 | dataset==102
 label var population_category "Population status"
 replace   population_category=2 if dataset>=103 & dataset<=108
 replace   population_category=3 if dataset>=109 & dataset<=116 
 
*rename position has_position

*rename juan juanna

* Juan originally had a lot of distinct codes according to what it prefixed.  We're collapsing it to a flag variable.
* Leave it in after all
* replace juan = 1 if juan > 1
rename dead died_with_title
rename estimated_income position_income	
rename sangfu_year age_widowed
 replace age_widowed=-98 if age_widowed==0
 replace age_widowed=age_widowed-birthyear+1 if age_widowed>1800  /*in sui*/
* Force to missing if birthyear is missing
 replace age_widowed=-98 if birthyear < 0

*! The original coding of sangfu_year is messy with many coded in widowing year instead of age in sui.  

label define titles 1 "bi tie shi" 2 "bi zheng" 3 "cui zhang" 4 "fang yu" 5 "guan xue sheng" 6 "jian sheng" 7 "jiao guan" 8 "jiao xi" 9 "jing sheng" 10 "ku shi" 11 "ling cui" 12 "mu zhang" 13 "pi jia" 14 "qian feng xiao" 15 "tai ling" 16 "xiao qi xiao" 17 "xie ling" 18 "xun dao" 19 "yun qi wei" 20 "zhang jing" 21 "zhi fu" 22 "zhi shi ren" 23 "zhu shi" 24 "zuo ling" 25 "fu du tong" 26 "jian fang fu du tong" 27 "zhi xian" 28 "tang zhu shi" 29 "xiao zheng"
label define titles 30 "gong" 31 "ying qian zong" 32 "fu yuan cheng xian" 33 "yi xu" 34 "jiu pin", add
label define titles 35 "ba pin" 36 "qi pin" 37 "liu pin" 38 "wu pin" 39 "si pin", add
label define titles 40 "san pin" 41 "er pin" 42 "yi pin" 43 "fu jing li xian" 44 "gong pai", add
label define titles 45 "gong sheng" 46 "hong mu" 47 "hou bu bie tie shi" 48 "hou bu cui zhang" 49 "hou bu ku shi", add
label define titles 50 "jing li zhi xian" 51 "kong ri" 52 "liu pin bi gong" 53 "xun cha" 54 "yi sheng" 55 "zan li lang xian" 56 "zhou tong xian" 57 "shou yu suo" 58 "tong pan" 59 "fu qian zong", add
label define titles 60 "si ku", add

label values hou_bu titles
label values juan titles
*rename unique_village unique_village_id

#delimit;
 label define event
 1	"wang gu"
2	"chu jia"
3	"gai jia"
4	"chu tao"
5	"qian chu"
6	"xin sheng"
7	"xin qu"
8	"ji chu"
9	"xin ru"
10	"Zhenmi"
11	"ji ru"
12	"xiao chu qi dang"
13	"hui jing"
14	"zheng fa"
15	"zheng hu"
16	"chu zheng"
17	"zhen wang"
18	"hui ji"
19	"zheng hu ru"
20	"zheng hu chu"
21	"wai chu bu hui"
22	"chu jia"
23	"feng wen zhen tao"
24	"feng wen ru ce"
25	"feng wen  gu"
26	"xiao chu mu dang"
27	"gao jia wei hui"
28	"qian hui"
29	"zhu si"
30	"zai ying"
31	"jun ying bing gu"
;
#delimit cr

 label val event_1 event_2 event

* We will put position_income back in later

drop position_income

rename zuoling_qi     original_admin
rename zuoling_diming original_place

destring original_place, replace

replace original_admin = -99 if original_admin == 0
replace original_place = -99 if original_place == 0

merge m:1 dataset original_place using original_place, keep(match master)

drop original_place _merge

encode place_name, generate(original_place)

drop place_name zhenghudizhi qianzhudi

sort record_number

write_files "`date_time'" basic_n_analytical

restore

* Now we just provide a consolidated basic and analytical file, so don't need following code

/*
preserve

keep adopted_in adopted_out bai_zong banner adopted birth_order birth_order_sex bi_tie_shi diminutive_name ding_dai disability_code estimated_income event_1 event_2 ever_absconded examination expelled founder_id founder_inferred_id gao_li dead has_surname hh_size hh_divide_next honorific hou_bu juan man_zhou non_han_name ling_cui man_zhou new_ding number_name original_admin place_of_origin population_category position_code purchased_title qian_zong rank record_number rustic_name seniority seniority_sex service_ding tou_cong unique_group unique_hh_id unique_yi_hu xiao_qi_xiao zhi_shi_ren 

order *, alphabetical

* Juan originally had a lot of distinct codes according to what it prefixed.  We're collapsing it to a flag variable.
* Leave it in after all

* replace juan = 1 if juan > 1

label define titles 1 "bi tie shi" 2 "bi zheng" 3 "cui zhang" 4 "fang yu" 5 "guan xue sheng" 6 "jian sheng" 7 "jiao guan" 8 "jiao xi" 9 "jing sheng" 10 "ku shi" 11 "ling cui" 12 "mu zhang" 13 "pi jia" 14 "qian feng xiao" 15 "tai ling" 16 "xiao qi xiao" 17 "xie ling" 18 "xun dao" 19 "yun qi wei" 20 "zhang jing" 21 "zhi fu" 22 "zhi shi ren" 23 "zhu shi" 24 "zuo ling" 25 "fu du tong" 26 "jian fang fu du tong" 27 "zhi xian" 28 "tang zhu shi" 29 "xiao zheng"
label define titles 30 "gong" 31 "ying qian zong" 32 "fu yuan cheng xian" 33 "yi xu" 34 "jiu pin", add
label define titles 35 "ba pin" 36 "qi pin" 37 "liu pin" 38 "wu pin" 39 "si pin", add
label define titles 40 "san pin" 41 "er pin" 42 "yi pin" 43 "fu jing li xian" 44 "gong pai", add
label define titles 45 "gong sheng" 46 "hong mu" 47 "hou bu bie tie shi" 48 "hou bu cui zhang" 49 "hou bu ku shi", add
label define titles 50 "jing li zhi xian" 51 "kong ri" 52 "liu pin bi gong" 53 "xun cha" 54 "yi sheng" 55 "zan li lang xian" 56 "zhou tong xian" 57 "shou yu suo" 58 "tong pan" 59 "fu qian zong", add
label define titles 60 "si ku", add

label values hou_bu titles
label values juan titles


write_files "`date_time'" analytic

restore
*/

preserve

keep record_number dataset /*unique_village*/ address surname_yihu /*region prefecture*/ zuoling_ming zhenghudizhi qianzhudi

merge 1:1 dataset record_number using "`name_file'", keep(match master) 
drop _merge

rename xing surname
rename ming given_name

rename hxing husband_surname
rename hming husband_given_name

rename fxing father_surname
rename fming father_given_name

rename gfming grandfather_given_name

sort dataset address

merge m:1 dataset address using "Village master", keep(match master) keepusing(lati longi) 
drop _merge 

replace lati = -999 if lati == . | lati == 0
replace longi = -999 if longi == . | longi == 0
*replace suoshu = -99 if suoshu == .
replace address = -99 if address == 0

rename address raw_village_id
*rename unique_village unique_village_id

rename lati latitude
rename longi longitude
rename zuoling_ming original_commander

replace original_commander = "" if index(original_commander,"?") > 0
replace original_commander = "" if original_commander == "1258" | original_commander == "1396" | original_commander == "2065" | original_commander == "2558"

*drop suoshu

merge m:1 dataset qianzhudi using qianzhudi

encode qianzhudi_pinyin, generate(fuding_destination)

drop qianzhudi_pinyin qianzhudi _merge

rename zhenghudizhi new_address
*drop region prefecture

sort record_number  
write_files "`date_time'" restricted

restore

sort record_number
keep record_number f_id_1-f_id_4 m_id_1-m_id_4 kin_m_1-kin_m_1_pa kin_f_1_ma kin_hh_m_1-kin_hh_m_1_pa kin_hh_f_1_ma kin_md_m_1-kin_md_m_1_pa kin_md_f_1_ma kin_hh_md_m_1-kin_hh_md_m_1_pa kin_hh_md_f_1_ma kin_wid_m_1-kin_wid_m_1_pa kin_wid_f_1_ma kin_hh_wid_m_1-kin_hh_wid_m_1_pa kin_hh_wid_f_1_ma

write_files "`date_time'" kinship

drop _all

use "Disability master" if dataset >= 101 & dataset <= 116 & disabil > 0
keep dataset disabil disease 
rename disabil disability_code
rename disease condition_pinyin

sort dataset disability_code 

write_files "`date_time'" disability

drop _all

use "Occupation master" if dataset >= 101 & dataset <= 116 & occu >= 1
keep dataset occu occuname core_occu

rename occu position_code
rename occuname position_pinyin
rename core_occu position_core

sort dataset position_code

write_files "`date_time'" position

drop _all

* The following isn't necessary since there is no rank variable in the original CMGPD-SC

/*

use "Rank master" if dataset >= 101 & rank >= 1
keep dataset rank rankname core_rank

rename rank position_2_code
rename rankname position_2_pinyin
rename core_rank position_2_core

sort dataset position_2_code
 
write_files "`date_time'" position_2

drop _all

*/

