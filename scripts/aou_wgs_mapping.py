### Calculate WGS counts of ICD10CM children code in European ancestry ###
### This code chunk:
### (1) read ./european_ancestry_children_individual folder and create "_keep_samples.txt" containing extract person_id 
### (2) read the selected 50000 target samples id
### (3) extract WGS sample count from "_keep_samples.txt" text without creating new .fam .bed .bim
### for both selected 50000 target samples and all European samples
### (4) write matched WGS sample count in corresponding "european_ancestry_ICD10CM_childrencode_WGS.csv" file

import os
import glob
import pandas as pd
import re

# get all ICD10CM .csv file
output_dir = os.path.join(os.getcwd(), "european_ancestry_children_individual")
csv_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith(".csv")]

# read the selected 50000 target samples id
sampleinformation_output_path = os.path.join(os.getcwd(), "european_ancestry_50000sample_id.csv")
sample_df = pd.read_csv(sampleinformation_output_path)
sample_id = {str(id) for id in sample_df['person_id']}
sample_id = {('0', sid) for sid in sample_id}

# create a id reference list for each ICD10CM code
for csv_file in csv_files:
    with open(csv_file, "r") as f:
        lines = f.readlines()
        sample_ids = [line.strip().split(",")[0] for line in lines[1:] if line.strip()]
        
    keep_lines = [f"0 {sid}" for sid in sample_ids]

    keep_name = os.path.splitext(csv_file)[0] + "_keep_samples.txt"
    keep_path = os.path.join(output_dir, keep_name)
    
    with open(keep_path, "w") as out_f:
        out_f.write("\n".join(keep_lines) + "\n")
        
# extract WGS sample count from reference text without creating new .fam .bed .bim
fam_file = os.path.join(os.getcwd(), "v8_plink_data/chr20.fam")
keep_dir = os.path.join(os.getcwd(), "european_ancestry_children_individual")
keep_files = glob.glob(os.path.join(keep_dir, "*_keep_samples.txt"))

results_allsamples = []
results_50000samples = []

with open(fam_file, "r") as f:
    fam_ids = set(tuple(line.strip().split()[:2]) for line in f)

for keep_file in keep_files:
    base_name = os.path.basename(keep_file).split("_keep_samples.txt")[0]
    
    with open(keep_file, "r") as f:
        keep_ids = set(tuple(line.strip().split()[:2]) for line in f)

    matched_allsamples = keep_ids & fam_ids
    matches_50000samples = keep_ids & fam_ids & sample_id
    results_allsamples.append({"ICD10_code": base_name, "WGS_case_count": len(matched_allsamples)})
    results_50000samples.append({"ICD10_code": base_name, "50000_case_count": len(matches_50000samples)})
    
code_matchcount_df = pd.DataFrame(results_allsamples)
code_matchcount_50000_df = pd.DataFrame(results_50000samples)

# write matched WGS sample count and sample counts in 50000 sample list with corresponding ICD10CM file
icd_file = os.path.join(os.getcwd(), "european_ancestry_ICD10CM_childrencode.csv")
icd_df = pd.read_csv(icd_file)

# write total case number with WGS of each trait to the file
merged_df = icd_df.merge(code_matchcount_df, on="ICD10_code", how="left")
merged_df["WGS_case_count"] = merged_df["WGS_case_count"].fillna(0).astype(int)

# write total case number in 50000 sample with WGS of each trait to the file
merged_df = merged_df.merge(code_matchcount_50000_df, on="ICD10_code", how="left")
merged_df["50000_case_count"] = merged_df["50000_case_count"].fillna(0).astype(int)

# change the colume position
cols = list(merged_df.columns)
cols.remove("WGS_case_count")
cols = cols[:2] + ["WGS_case_count"] + cols[2:]
merged_df = merged_df[cols]
cols = list(merged_df.columns)
cols.remove("50000_case_count")
cols = cols[:3] + ["50000_case_count"] + cols[3:]
merged_df = merged_df[cols]

merged_df.to_csv(os.path.join(os.getcwd(), "ICD10CM_childrencode_WGS.csv"), index=False)

### mapping with FinnGen_ICD10_426_fixed.csv ###

current_dir = os.getcwd()

df_rootcode_wgs = pd.read_csv(os.path.join(current_dir, "european_ancestry_ICD10CM_childrencode_WGS.csv"))
df_finngen_icd = pd.read_csv(os.path.join(current_dir, "FinnGen_ICD10_426_fixed.csv"))

df_finngen_icd = df_finngen_icd.dropna(subset=["ICD10", "phenocode"])
df_finngen_icd["ICD10"] = df_finngen_icd["ICD10"].astype(str)

codes_top500 = pd.DataFrame(df_rootcode_wgs["ICD10_code_original"].head(500).unique(), columns=["ICD10_code_original"])
codes_top500["ICD10_code"] = codes_top500["ICD10_code_original"].astype(str)

matched_rows = []
for idx, row in codes_top500.iterrows():
    code_to_match = row["ICD10_code"]
    for idx2, rw in df_finngen_icd.iterrows():
        icd10_codes = str(rw["ICD10"]).split(",")
        if code_to_match in icd10_codes:
            matched_rows.append({"ICD10_code": code_to_match, "phenocode": rw["phenocode"]})

matched_df = pd.DataFrame(matched_rows)
matched_df.to_csv(os.path.join(current_dir, "european_ancestry_top500childrencode_icd_to_phenocode_426.csv"), index=False)

### another mapping strategy ###
### mapping with icd10_to_phecode_may4.csv ###

current_dir = os.getcwd()

# df_childrencode_wgs = pd.read_csv(os.path.join(current_dir, "european_ancestry_ICD10CM_childrencode_WGS.csv"))
df_childrencode_wgs = pd.read_csv(os.path.join(current_dir, "ICD10CM_childrencode_WGS.csv"))
df_finngen_icd = pd.read_csv(os.path.join(current_dir, "icd10_to_phecode_may4.csv"))

df_finngen_icd = df_finngen_icd.dropna(subset=["ICD10", "phenocode"])
df_finngen_icd["ICD10"] = df_finngen_icd["ICD10"].astype(str)

# choose the top n traits with the most number of cases
codes_top500 = pd.DataFrame(df_childrencode_wgs["ICD10_code_original"].head(2000).unique(), columns=["ICD10_code_original"])
codes_top500["ICD10_code"] = codes_top500["ICD10_code_original"].astype(str)

matched_rows = []
for idx, row in codes_top500.iterrows():
    code_to_match = row["ICD10_code"]
    for idx2, rw in df_finngen_icd.iterrows():
        if code_to_match == rw["ICD10"]:
            matched_rows.append({"ICD10_code": code_to_match, "phenocode": rw["phenocode"]})
            print(code_to_match, rw["phenocode"])

matched_df = pd.DataFrame(matched_rows)
matched_df.to_csv(os.path.join(current_dir, "top2000childrencode_icd_to_phenocode_may4.csv"), index=False)

