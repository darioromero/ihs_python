import re
from datetime import datetime


# dictionary with 298 Production Export codes
codes = {
    "Start Record Label": ["STA", [(31, 40)]],
    "Unique ID Record": ["++ ", [(4, 40)]],
    "Entity Record": "+A ",
    "Congressional and Carter Location": "+AC",
    "Texas Location": "+AT",
    "Offshore Location": "+AO",
    "Regulatory Record": "+AR",
    "Multiple County Record": "+A#",
    "Name Record 1": "+B ",
    "Name Record 2": "+C ",
    "Well Record": ["+D ", [(4, 15), (22, 9), (31, 5), (58, 1), (59, 1), (66, 5)]],
    "Lat/Long Record": ["+D!", [(4, 9), (13, 10)]],
    "Test Information Record 1": "+E ",
    "Test Information Record 2": "+E!",
    "Cumulative Production": "+F ",
    "Monthly Production": ["+G ", [(4, 8), (12, 15), (27, 15), (42, 15)]],
    "Cumulative Injection": "+I ",
    "Monthly Injection": "+J ",
    "Total Disposition for Current Month": "+K ",
    "Monthly Disposition by Transporter": "+L ",
    "End Record Label": ["END", []]
}

print("----------------------------------------------------------------------------------------------------")
start_dt = datetime.now()
print("starting ...")


# creates production csv output file
f_out_prod = open("data/298fProductionOutput.csv", "w")
print("  opening file (production): ", f_out_prod.name)
f_out_prod.write('uid, api_number, date_prod, oil_bbl, gas_mcf, water_bbl, lat, lon, td, tvd\n')


def get_token_by_id(code, idx):
    idx1 = codes[code][1][idx][0] - 1
    idx2 = codes[code][1][idx][0] - 1 + codes[code][1][idx][1]
    return idx1, idx2


# initialize num_wells, non_multi wells, and active wells
num_wells = 0
non_multi = 0
num_wells_active = 0
well_status = "I"
uid = ""
api = ""
multi_well = True
api_number = ""
surf_lat = 0.0
surf_lon = 0.0
td = 0.0
tvd = 0.0

in_file = ''
in_read = True
while in_read:
    # in_file = input('Type the filename: \n')
    in_file = "data/output.98f"
    if '98f' in in_file.split('.'):
        in_read = False
        with open(in_file, "r") as f_input:
            num_line = 0
            for line in f_input:
                num_line += 1
                line = (re.split('\n', line)[0])
                curr_code = line[0:3]

                # "Start Record Label 'STA'"
                if curr_code == codes["Start Record Label"][0]:
                    num_wells += 1
                    # count non-MULTI well
                    if "MULTI" not in line:
                        non_multi += 1
                        multi_well = not multi_well

                # "Unique ID Record '++ '"
                if curr_code == codes["Unique ID Record"][0]:
                    first, last = get_token_by_id('Unique ID Record', 0)
                    uid = line[first:last].strip(' ')

                # "Well Record '+D '"
                if curr_code == codes["Well Record"][0]:
                    first, last = get_token_by_id('Well Record', 0)
                    api_number = line[first:last].strip(' ')
                    first, last = get_token_by_id('Well Record', 1)
                    well_number = line[first:last]

                    first, last = get_token_by_id('Well Record', 2)
                    td = line[first:last].strip(' ')
                    first, last = get_token_by_id('Well Record', 3)
                    dir_drill = line[first:last]

                    # set and count active well indicator
                    # 8: +D ,"","","",,,,"A0","V","I","","",,"465"
                    first, last = get_token_by_id('Well Record', 4)
                    well_status = line[first:last]
                    if well_status == "A":
                        num_wells_active += 1
                    first, last = get_token_by_id('Well Record', 5)
                    tvd = line[first:last].strip(' ')

                # "Lat/Long Record '+D!'"
                if curr_code == codes["Lat/Long Record"][0] and well_status == "A" and not multi_well:
                    # lat and long coordinates
                    first, last = get_token_by_id('Lat/Long Record', 0)
                    surf_lat = line[first:last].strip(' ')
                    first, last = get_token_by_id('Lat/Long Record', 1)
                    surf_lon = line[first:last].strip(' ')

                # "Monthly Production '+G '"
                if curr_code == codes["Monthly Production"][0] and well_status == "A" and not multi_well:
                    # add uid to monthly production record
                    first, last = get_token_by_id('Monthly Production', 0)
                    date_prod = line[first:last]

                    first, last = get_token_by_id('Monthly Production', 1)
                    liquid_prod = line[first:last].strip(' ')

                    first, last = get_token_by_id('Monthly Production', 2)
                    gas_prod = line[first:last].strip(' ')

                    first, last = get_token_by_id('Monthly Production', 3)
                    water_prod = line[first:last].strip(' ')

                    f_out_prod.write('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}\n'.
                                     format(uid, api_number, date_prod, liquid_prod,gas_prod, water_prod,
                                            surf_lat, surf_lon, td, tvd))

                # "End Record Label 'END'"
                if curr_code == codes["End Record Label"][0]:
                    continue

    else:
        print("File is not a FIXED formatted type")

print(f'# Wells processed: [{num_wells}]')
print("---------------")
f_input.close()


# closing file for writing
print("  \nclosing files: (production) ...")
f_out_prod.close()

end_dt = datetime.now()
print("----------------------------------------------------------------------------------------------------")
print("start: ", start_dt, " -- end: ", end_dt, " -- Elapsed: ",
      (end_dt - start_dt))




