import os
from datetime import datetime


class WellStatus(object):
    ACTIVE = '"A"'
    INACTIVE = '"I"'


# dictionary with 298 Production Export codes
codes = {
    "Start Record Label": "START_US_PROD",
    "Unique ID Record": "++ ",
    "Entity Record": "+A ",
    "Congressional and Carter Location": "+AC",
    "Texas Location": "+AT",
    "Offshore Location": "+AO",
    "Regulatory Record": "+AR",
    "Multiple County Record": "+A#",
    "Name Record 1": "+B ",
    "Name Record 2": "+C ",
    "Well Record": "+D ",
    "Lat/Long Record": "+D!",
    "Test Information Record 1": "+E ",
    "Test Information Record 2": "+E!",
    "Cumulative Production": "+F ",
    "Monthly Production": "+G ",
    "Cumulative Injection": "+I ",
    "Monthly Injection": "+J ",
    "Total Disposition for Current Month": "+K ",
    "Monthly Disposition by Transporter": "+L ",
    "End Record Label": "END_US_PROD"
}

print("----------------------------------------------------------------------------------------------------")
start_dt = datetime.now()
print("starting ...")

# open production ihs export file for read-only - fixed-name file 298fProduction.98c
finput = open("../../../Users/Dario/Documents/IHS/298fProduction.98c", "r")

# read production ihs export file
lines = finput.readlines()
print("    number of read lines: ", len(lines))

# close production ihs-export input file
finput.close()

# initialize num_wells, non_multi wells, and active wells
num_wells = 0
non_multi = 0
num_wells_active = 0

# creates header csv output file
fout_head = open("298fHeaderOutput.csv", "w")
print("  opening file (header):", fout_head.name)
fout_head.write('id, uid, uid_source, well_name, region_code, state_code, field_code, '
                'county_parish_code, county_parish_name, operator_code, primary_prod_code, '
                'mode, formation_code, res_code, surf_lat, surf_lon\n')

# creates production csv output file
fout_prod = open("298fProductionOutput.csv", "w")
print("  opening file (production): ", fout_prod.name)
fout_prod.write('id, pdate, liquid_bbl, gas_mcf, water_bbl, allowable, nwells, dop\n')

# creates production test csv output file
fout_test = open("298fTestOutput.csv", "w")
print("  opening file (test): ", fout_test.name)
fout_test.write('id, testNumber, testDate, upperPerfDepth, lowerPerfDepth, liquidsPerDay, '
                'gasPerDay, waterPerDay, chokeSize, bsw, ftp, gor, liquidGravity, '
                'finalSIP, gasGravity, prodMethod, bhpOverZ, zFactor, nFactor, calcAOF, '
                'cumGas,clp\n')

if lines[0].split()[7] == "COMMA":
    tinfo = 0
    for line in lines:
        # if new well found -- uid is initialized
        # increment num_wells
        # initialize uid, wellstatus, res_name
        if codes["Start Record Label"] in line:
            num_wells += 1
            entid = []
            uid = ''.split()
            temp = ''.split()
            wellstatus = ""
            reservoir = ""
            res_name = ""
            multi_well = True
            state = ""
            county = ""
            api = ""
            lease = ""
            # cont non-MULTI well
            if not line.split(',')[1].__contains__("MULTI"):
                non_multi += 1
                multi_well = not multi_well
        if codes["Unique ID Record"] in line:
            token = line.split(',')
            uid = token[1]
            uid_source = token[2].strip('\n')
        if codes["Entity Record"] in line:
            token = line.split(',')
            region_code = token[1]
            state_code = token[2]
            field_code = token[3]
            county_parish_code = token[4]
            county_parish_name = token[5]
            operator_code = token[6]
            primary_prod_code = token[7]
            mode = token[8]
            formation_code = token[9]
            aapg_basin_code = token[10]
            cbm_ind = token[11]
        if codes["Congressional and Carter Location"] in line:
            continue
        if codes["Texas Location"] in line:
            token = line.split(',')
            survey_name = token[11]
            abstract_num = token[12]
        if codes["Offshore Location"] in line:
            continue
        if codes["Regulatory Record"] in line:
            token = line.split(',')
            lease_unit_code = token[1]
            serial_num = token[2]
            res_code = token[5]
            state_offshore_flag = token[6]
            api_unique = token[7]
            district_code = token[8]
        if codes["Multiple County Record"] in line:
            token = line.split(',')
            formation_name = token[7]
        if codes["Name Record 1"] in line:
            token = line.split(',')
            lease_name = token[1]
            operator_name = token[2]
        if codes["Name Record 2"] in line:
            token = line.split(',')
            field_name = token[1]
            # set reservoir name
            reservoir_name = token[5].strip('\n')
        if codes["Well Record"] in line:
            token = line.split(',')
            api_number = token[1]
            well_number = token[3]
            # set and count active well indicator
            # 8: +D ,"","","",,,,"A0","V","I","","",,"465"
            td = token[4]
            dirdrill = token[8]
            wellstatus = token[9]
            tvd = token[12]
            if wellstatus == WellStatus.ACTIVE:
                num_wells_active += 1
        if codes["Lat/Long Record"] in line and wellstatus == WellStatus.ACTIVE and not multi_well:
            token = line.split(',')
            # lat and long coordinates
            surf_lat = token[1]
            surf_lon = token[2]
            id_unique = '"' + ("{0}{1}".format(api_number.strip('"'), lease_unit_code.strip('"'))) + '"'
            lease_name2 = '"' + lease_name.strip('"') + ' #' + well_number.strip('"') + '"'
            plist = [uid, uid_source, lease_name2, region_code, state_code, field_code, county_parish_code,
                     county_parish_name, operator_code, primary_prod_code, mode, formation_code, res_code,
                     surf_lat, surf_lon]
            fout_head.write(id_unique + ',' + (','.join(plist)) + '\n')
        if codes["Test Information Record 1"] in line:
            token = line.split(',')
            token[len(token) - 1] = token[len(token) - 1].strip('\n')
            plist = [str(token[1]), str(token[15])]
            t1 = ','.join(map(str, token[2:15]))  # t1 has 13 members
            tinfo += 1
            print('Test Info 1 - API: {0} - Test: {1}'.format(api_number.strip('"'), tinfo))
            print('t1: ' + t1)
            print('--------------------------------------------------------------------------------')
        if codes["Test Information Record 2"] in line:
            token = line.split(',')
            t2 = ','.join(map(str, token[2:len(token)]))  # t2 has 7 members
            tinfo += 1
            print('Test Info 2 - API: {0} - Test: {1}'.format(api_number.strip('"'), tinfo))
            print('t2: ' + t2)
            print('--------------------------------------------------------------------------------')
            id_unique = '"' + ("{0}{1}".format(api_number.strip('"'), lease_unit_code.strip('"'))) + '"'
            fout_test.write(id_unique + ',' + ','.join(map(str, plist)) + ',' + t1 + ',' + t2)
        if codes["Cumulative Production"] in line:
            continue
        if codes["Monthly Production"] in line and wellstatus == WellStatus.ACTIVE and not multi_well:
            token = line.split(',')
            # add uid to monthly production record
            date_prod = token[1]
            liquid_prod = token[2]
            gas_prod = token[3]
            water_prod = token[4]
            allow_prod = token[5]
            num_wells_prod = token[6]
            id_unique = '"' + ("{0}{1}".format(api_number.strip('"'), lease_unit_code.strip('"'))) + '"'
            fout_prod.write(id_unique + ',' + ','.join(token[1:8]))
        if codes["Cumulative Injection"] in line:
            continue
        if codes["Monthly Injection"] in line:
            continue
        if codes["Total Disposition for Current Month"] in line:
            continue
        if codes["Monthly Disposition by Transporter"] in line:
            continue
        if codes["End Record Label"] in line:
            continue
else:
    print("File non-comma separated")

# closing file for writing
print("  closing file (header) ...")
fout_head.close()
print("  closing file (test) ...")
fout_test.close()
print("  closing file (production) ...")
fout_prod.close()

print('------------')
print("  # wells: ", num_wells, ";    non-multi wells: ", non_multi, ";    active wells: ", num_wells_active)

end_dt = datetime.now()
print("----------------------------------------------------------------------------------------------------")
print("start: ", start_dt, " -- end: ", end_dt, " -- Elapsed: ",
      (end_dt - start_dt))




