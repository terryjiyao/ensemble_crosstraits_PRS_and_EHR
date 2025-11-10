### Organize upload PRS model ###
### This code chunk:
### (1) interate all file in prsmodel folder 
### (2) unzip .zip file if nessceary 
### (3) perpare .score file for calculateing PRS file with PLINK
### (4) save list of all available model and model_path

import os
import re
import pandas as pd
import zipfile

# prsmodel folder
target_dir = os.path.join(os.getcwd(), "prsmodel")
model_list = []
model_path_list = []

# initial screening of all file
file_list = os.listdir(target_dir)

# iterate all .zip file
i = 0
while i < len(file_list):
    filename = file_list[i]
    print(filename)
    zip_path = os.path.join(target_dir, filename)
    
    # if its a zip file
    if filename.endswith(".zip"):
        # check if its a fine zip file
        if not zipfile.is_zipfile(zip_path):
            print(f"skip cauze not a full zip file: {filename}")
            i += 1
            continue
        
        # generate folder to save PRS model
        folder_name = filename.replace(".zip", "")
        extract_dir = os.path.join(target_dir, folder_name)
        
        # skip if folder already exists and is not empty
        if os.path.exists(extract_dir) and os.listdir(extract_dir):
            print(f"skip cauze already unziped: {extract_dir}")
            i += 1
            continue
        
        # if not, create new folder and unzip .zip file
        else:
            try:
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                os.remove(zip_path)
                print(f"Finish unzip:{filename} → {extract_dir}")
                
                # put the new folder in the iteration list
                file_list.append(folder_name)
                
            except zipfile.BadZipFile:
                print(f"fail to unzip:{filename}")
            except Exception as e:
                print(f"failure ({filename}):{e}")
    
    # if its a folder and it is not empty
    if os.path.isdir(zip_path) and os.listdir(zip_path):
        # fetch phenocode
        match = re.search(r"EUR_finngen_R12_(.+?)_EUR", filename)
        if match:
            # append model path and model name to list
            extracted_str = match.group(1)
            model_list.append(extracted_str)
            model_path_list.append(zip_path)
            
            # Check if any .score file already exists in the folder
            score_files_exist = False
            for f in os.listdir(zip_path):
                if f.endswith(".score"):
                    score_files_exist = True
                    print(f"Skipping this folder because .score file already exists: {f}")
                    break

            # If no .score files exist, proceed to prepare .score files
            if not score_files_exist:
                # perpare .score file
                model_type_map = {
                    f"EUR_finngen_R12_{extracted_str}.C+T-pseudo.PRS.txt": "C+T-pseudo",
                    f"EUR_finngen_R12_{extracted_str}.lassosum2-pseudo.PRS.txt": "lassosum2-pseudo",
                    f"EUR_finngen_R12_{extracted_str}.LDpred2-pseudo.PRS.txt": "LDpred2-pseudo",
                    f"EUR_finngen_R12_{extracted_str}.ensemble_PRS_by_model_averaging.txt": "ensemble_PRS_by_model_averaging",
                    f"EUR_finngen_R12_{extracted_str}.ensemble_PRS.txt": "ensemble_PRS"
                }

                for model_file, model_type in model_type_map.items():
                    model_path = os.path.join(zip_path, model_file)
                    if os.path.exists(model_path):
                        df = pd.read_csv(model_path, delim_whitespace=True)
                        if 'BETA' in df.columns:
                            score_df = df[['SNP', 'A1', 'BETA']]
                            score_file = f"{extracted_str}_{model_type}.score"
                            score_path = os.path.join(zip_path, score_file)
                            score_df.to_csv(score_path, sep=' ', index=False)
                        else:
                            print(f"{model_file} does not contain 'BETA' column.")
                print(f"✔ Finished extracting score files for {extracted_str}")
            else:
                print(f"✔ Skipped extracting score files for {extracted_str}, .score file exists.")
    i += 1

# add model availablity to icd10-phenocode pair

csv_path = os.path.join(os.getcwd(), "top2000childrencode_icd_to_phenocode_may4.csv")
df = pd.read_csv(csv_path)
model_set = set(model_list)
df["ava_model"] = df["phenocode"].apply(lambda x: 1 if x in model_set else 0)
df.to_csv(os.path.join(os.getcwd(), "top2000childrencode_icd_to_phenocode_may4.csv"), index=False)

# check RAM availability
import shutil

!rm -rf /home/jupyter/.local/share/Trash/*
total, used, free = shutil.disk_usage("/home/jupyter/workspaces/controlledtierdatasetv8/")

print(f"total space: {total // (1024**3)} GB")
print(f"occupied space:   {used // (1024**3)} GB")
print(f"available space:   {free // (1024**3)} GB")

### This code chunk is for extracting WGS data of 50000 european samples in all 22 chr ###
# (only need to run once)

df = pd.read_csv("/home/jupyter/workspaces/controlledtierdatasetv8/european_ancestry_50000sample_id.csv")
df_keep = pd.DataFrame({
    "FID": 0,
    "IID": df["person_id"]
})
keep_file = "/home/jupyter/workspaces/controlledtierdatasetv8/european_ancestry_50000sample_id.keep"
df_keep.to_csv(keep_file, sep=" ", index=False, header=False)
local_dir = "/home/jupyter/workspaces/controlledtierdatasetv8/acaf_threshold"

for chr_num in range(1, 23):  
    chr_str = f"chr{chr_num}"
    print(f"\nProcessing {chr_str} ...")
    
    !plink \
      --bfile {local_dir}/{chr_str}_hm3_38 \
      --keep {keep_file} \
      --make-bed \
      --out {local_dir}/{chr_str}_hm3_38_european_ancestry_50000

### exclude multiallelic SNPs in each chromesome ###
# (only need to run once)

merge_list_path = f"{local_dir}/merge_list.txt"

with open(merge_list_path, "w") as f:
    for i in range(2, 23):
        f.write(f"{local_dir}/chr{i}_hm3_38_european_ancestry_50000\n")
        
!cat {merge_list_path} 
!plink --bfile {local_dir}/chr1_hm3_38_european_ancestry_50000 \
       --merge-list {local_dir}/merge_list.txt \
       --make-bed \
       --out {local_dir}/all_european_ancestry_50000

for i in range(1, 23):
    !plink --bfile {local_dir}/chr{i}_hm3_38_european_ancestry_50000 \
      --exclude {local_dir}/all_european_ancestry_50000-merge.missnp \
      --make-bed \
      --out {local_dir}/chr{i}_hm3_38_european_ancestry_50000_exclude_multiallelic

### merge all SNPs to one file (without multiallelic) ###
# (only need to run once)

merge_list_path = f"{local_dir}/merge_list_exclude_multiallelic.txt"
with open(merge_list_path, "w") as f:
    for i in range(2, 23):
        f.write(f"{local_dir}/chr{i}_hm3_38_european_ancestry_50000_exclude_multiallelic\n")

!cat {merge_list_path} 
!plink --bfile {local_dir}/chr1_hm3_38_european_ancestry_50000_exclude_multiallelic \
       --merge-list {local_dir}/merge_list_exclude_multiallelic.txt \
       --make-bed \
       --out {local_dir}/all_european_ancestry_50000_exclude_multiallelic

# check SNPs information
print("How many SNPs in HM3 have?")
!wc -l /home/jupyter/workspaces/controlledtierdatasetv8/hapmap3rsid.txt
print("How many SNPs in total (22 chromesome excluding multiallelic SNPs)?")
!wc -l /home/jupyter/workspaces/controlledtierdatasetv8/acaf_threshold/all_european_ancestry_50000_exclude_multiallelic.bim
print("How many sample available (22 chromesome excluding multiallelic SNPs)?")
!wc -l /home/jupyter/workspaces/controlledtierdatasetv8/acaf_threshold/all_european_ancestry_50000_exclude_multiallelic.fam

fam_path = "/home/jupyter/workspaces/controlledtierdatasetv8/acaf_threshold/all_european_ancestry_50000_exclude_multiallelic.fam"
fam_df = pd.read_csv(fam_path, delim_whitespace=True, header=None)
fam_iids = set(fam_df[1].astype(str))

csv_path = "/home/jupyter/workspaces/controlledtierdatasetv8/european_ancestry_50000sample_id.csv"
csv_df = pd.read_csv(csv_path)
csv_ids = set(csv_df['person_id'].astype(str))

if fam_iids == csv_ids:
    print("Sample id in WGS and EHR are the same!")
else:
    print("Sample id in WGS and EHR not the same!")


### run all PRS model on 50000 WGS sample ###

csv_path = os.path.join(os.getcwd(), "top2000childrencode_icd_to_phenocode_may4.csv")
wgs_path = os.path.join(os.getcwd(), "acaf_threshold")
df = pd.read_csv(csv_path)
df_run_list = df[df["ava_model"] == 1]
run_list = set(df_run_list["phenocode"]) & set(model_list)

for i, phenocode in enumerate(run_list):
    print(f"----- start processing {phenocode} ({i} out of {len(run_list)})-----\n")
    dirname = f"EUR_finngen_R12_{phenocode}_EUR_C+T-pseudo.lassosum2-pseudo.LDpred2-pseudo_wang4421_finn"
    modeldir_path = os.path.join(os.getcwd(), "prsmodel", dirname)
    
    # perpare .score file for cacluating PRS score with PLINK
    model_type = {
        f"{phenocode}_C+T-pseudo.score",
        f"{phenocode}_lassosum2-pseudo.score",
        f"{phenocode}_LDpred2-pseudo.score",
        f"{phenocode}_ensemble_PRS_by_model_averaging.score",
        f"{phenocode}_ensemble_PRS.score"
    }

    for model_file in model_type:
        model_path = os.path.join(modeldir_path, model_file)
        
        # skip if there is no model
        if not os.path.exists(model_path):
            continue
        
        score_name = model_file.split(".")[0]  # e.g., 'E10_C+T-pseudo'
        out_path = os.path.join(modeldir_path, f"{score_name}_european_ancestry_50000_results")
                    
        # calculate PRS score using WGS information extracted
        !plink2 \
          --bfile {os.path.join(wgs_path, "all_european_ancestry_50000_exclude_multiallelic")} \
          --score {model_path} 1 2 3 header cols=+scoresums,+scoreavgs \
          --out {out_path}
    
    print(f"✔ Finished runing PRS score files for {phenocode}")
