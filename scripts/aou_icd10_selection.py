import pandas
import os

# This query represents dataset "White_ancestry_EHR" for domain "person" and was generated for All of Us Controlled Tier Dataset v8
dataset_39406143_person_sql = """
    SELECT
        person.person_id,
        person.gender_concept_id,
        p_gender_concept.concept_name as gender,
        person.birth_datetime as date_of_birth,
        person.race_concept_id,
        p_race_concept.concept_name as race,
        person.ethnicity_concept_id,
        p_ethnicity_concept.concept_name as ethnicity,
        person.sex_at_birth_concept_id,
        p_sex_at_birth_concept.concept_name as sex_at_birth,
        person.self_reported_category_concept_id,
        p_self_reported_category_concept.concept_name as self_reported_category 
    FROM
        `""" + os.environ["WORKSPACE_CDR"] + """.person` person 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_race_concept 
            ON person.race_concept_id = p_race_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_ethnicity_concept 
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` p_self_reported_category_concept 
            ON person.self_reported_category_concept_id = p_self_reported_category_concept.concept_id  
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) )"""

dataset_39406143_person_df = pandas.read_gbq(
    dataset_39406143_person_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_39406143_person_df.head(5)

import pandas
import os

# This query represents dataset "White_ancestry_EHR" for domain "condition" and was generated for All of Us Controlled Tier Dataset v8
dataset_39406143_condition_sql = """
    SELECT
        c_occurrence.person_id,
        c_occurrence.condition_concept_id,
        c_standard_concept.concept_name as standard_concept_name,
        c_standard_concept.concept_code as standard_concept_code,
        c_standard_concept.vocabulary_id as standard_vocabulary,
        c_occurrence.condition_start_datetime,
        c_occurrence.condition_end_datetime,
        c_occurrence.condition_type_concept_id,
        c_type.concept_name as condition_type_concept_name,
        c_occurrence.stop_reason,
        c_occurrence.visit_occurrence_id,
        visit.concept_name as visit_occurrence_concept_name,
        c_occurrence.condition_source_value,
        c_occurrence.condition_source_concept_id,
        c_source_concept.concept_name as source_concept_name,
        c_source_concept.concept_code as source_concept_code,
        c_source_concept.vocabulary_id as source_vocabulary,
        c_occurrence.condition_status_source_value,
        c_occurrence.condition_status_concept_id,
        c_status.concept_name as condition_status_concept_name 
    FROM
        ( SELECT
            * 
        FROM
            `""" + os.environ["WORKSPACE_CDR"] + """.condition_occurrence` c_occurrence 
        WHERE
            (
                condition_concept_id IN (SELECT
                    DISTINCT c.concept_id 
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` c 
                JOIN
                    (SELECT
                        CAST(cr.id as string) AS id       
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_criteria` cr       
                    WHERE
                        concept_id IN (133468, 134057, 134736, 135930, 138239, 138525, 140190, 141960, 192438, 193460, 194133, 194526, 200174, 200219, 200452, 201618, 201891, 253549, 254068, 254761, 255919, 31610, 316866, 31821, 318800, 320128, 320136, 321588, 37018424, 37018677, 37018713, 37203927, 37206233, 372887, 37311677, 37311678, 373499, 375252, 376106, 376208, 376337, 378253, 4000609, 4000610, 4002905, 4006969, 4009890, 4011630, 4013518, 4020346, 4021667, 4021780, 4021915, 4022201, 4022922, 4022923, 4022924, 4023995, 4024000, 4024013, 4024561, 4024566, 4024567, 4025215, 4027384, 4027553, 4028071, 4028076, 4028387, 4031814, 4038502, 4038677, 4038678, 4041283, 4041284, 4041285, 4041436, 4042056, 4042138, 4042141, 4042142, 4042503, 4042505, 4042835, 4042836, 4042837, 4043346, 4043371, 4043671, 40480457, 40481517, 40481841, 40484102, 40484533, 40484935, 4051221, 4054501, 4071689, 4080992, 4081176, 4082416, 4083787, 4083964, 4086181, 4090426, 4090614, 4090615, 4091213, 4091363,
 4091532, 4093227, 4093228, 4093991, 4095793, 4096864, 4101212, 4101343, 4101796, 4102111, 4103183, 4103320, 4103352, 4104157, 4113563, 4113999, 4115259, 4115386, 4115390, 4116811, 4117779, 4117930, 4132926, 4134294, 4134440, 4150129, 4162282, 4168335, 4170143, 4170226, 4170962, 4171379, 4175154, 4176644, 4178545, 4178680, 4178818, 4179141, 4179167, 4179871, 4179873, 4179922, 4180154, 4180169, 4180170, 4180628, 4181063, 4181187, 4182161, 4182165, 4182633, 4184252, 4185503, 4185640, 4190185, 4197094, 4198525, 4201745, 4206591, 4208786, 4212577, 4213101, 4221108, 4227253, 4244662, 4247371, 42538830, 4260918, 4266186, 4266188, 4267519, 4267789, 4269314, 4274025, 4276569, 4276572, 4277352, 4293175, 4297887, 4301699, 43021226, 4302537, 4304916, 4305027, 4308811, 4309188, 4316083, 4317258, 4318379, 432250, 432453, 432586, 432795, 432867, 4329041, 433128, 433736, 4338120, 4339410, 4339468, 4344497, 43531053, 43531054, 43531056, 43531057, 43531058, 43531059, 43531060, 435506, 435524, 436096,
 436670, 437515, 438112, 440029, 440142, 440371, 440383, 440921, 441542, 442077, 443343, 443723, 443783, 443784, 443883, 444089, 444100, 444108, 444112, 444363, 44782620, 44783587, 44784102, 73553, 75865, 75909, 77074, 77670, 77960, 80180)       
                        AND full_text LIKE '%_rank1]%'      ) a 
                        ON (c.path LIKE CONCAT('%.', a.id, '.%') 
                        OR c.path LIKE CONCAT('%.', a.id) 
                        OR c.path LIKE CONCAT(a.id, '.%') 
                        OR c.path = a.id) 
                WHERE
                    is_standard = 1 
                    AND is_selectable = 1)
            )  
            AND (
                c_occurrence.PERSON_ID IN (SELECT
                    distinct person_id  
                FROM
                    `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` cb_search_person  
                WHERE
                    cb_search_person.person_id IN (SELECT
                        person_id 
                    FROM
                        `""" + os.environ["WORKSPACE_CDR"] + """.cb_search_person` p 
                    WHERE
                        has_ehr_data = 1 ) )
            )) c_occurrence 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_standard_concept 
            ON c_occurrence.condition_concept_id = c_standard_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_type 
            ON c_occurrence.condition_type_concept_id = c_type.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.visit_occurrence` v 
            ON c_occurrence.visit_occurrence_id = v.visit_occurrence_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` visit 
            ON v.visit_concept_id = visit.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_source_concept 
            ON c_occurrence.condition_source_concept_id = c_source_concept.concept_id 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` c_status 
            ON c_occurrence.condition_status_concept_id = c_status.concept_id"""

dataset_39406143_condition_df = pandas.read_gbq(
    dataset_39406143_condition_sql,
    dialect="standard",
    use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),
    progress_bar_type="tqdm_notebook")

dataset_39406143_condition_df.head(5)

import pandas as pd
# merge race, sex, and date_of birth to target dataframe
analysis_df = pd.merge(dataset_39406143_condition_df, dataset_39406143_person_df[['person_id', 'race']], on='person_id', how='left')
analysis_df = pd.merge(analysis_df, dataset_39406143_person_df[['person_id', 'sex_at_birth']], on='person_id', how='left')
analysis_df = pd.merge(analysis_df, dataset_39406143_person_df[['person_id', 'date_of_birth']], on='person_id', how='left')

# load mapping csv
file_path = os.path.join(os.path.dirname(os.getcwd()), "references", "phecodeX_unrolled_ICD_CM.csv") # Print current working directory
map_df = pandas.read_csv(file_path)  # Load the CSV file into a DataFrame
map_ICD10_phecode_df = map_df[map_df['vocabulary_id'] == 'ICD10CM']

# load ICD10CM code txt
file_path = os.path.join(os.path.dirname(os.getcwd()), "references", "icd10cm-codes-April-2025.txt") # Print current working directory
codes = []
descriptions = []
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split(' ', 1) 
        if len(parts) == 2:
            code, desc = parts
            codes.append(code)
            descriptions.append(desc)
icd10cm_code = pandas.DataFrame({
    'code': codes,
    'description': descriptions
})

# load ICD10CM order txt
file_path = os.path.join(os.getcwd(), "icd10cm-order-April-2025.txt") # Print current working directory
orders = []
codes = []
descriptions = []
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split(' ', 5)  
        if len(parts) >= 4:  
            orders.append(parts[0]) 
            codes.append(parts[1])
            descriptions.append(parts[5])
icd10cm_order = pandas.DataFrame({
    'order': orders,
    'code': codes,
    'description': descriptions
})

### preprocess data ###

# all data counts
person_num = analysis_df['person_id'].value_counts()
sample_num = len(analysis_df)
print("Individual counts: ", len(person_num), "Sample counts: ", sample_num)

# remove duplicate samples
analysis_df = analysis_df.drop_duplicates(subset=[
    'person_id',
    'source_vocabulary', 
    'source_concept_code'
])

# extract all white individual 
analysis_df = analysis_df[analysis_df['race'] == 'White']

# calculate age (base on 2025)
analysis_df['date_of_birth'] = pd.to_datetime(analysis_df['date_of_birth'])
analysis_df['age_2025'] = 2025 - analysis_df['date_of_birth'].dt.year
analysis_df = analysis_df.drop(columns=['date_of_birth'])

# extract all sample with source vocabulary ICD10CM
analysis_df = analysis_df[analysis_df['source_vocabulary'] == 'ICD10CM']

print("Filtering European ancestry, removing duplicates, and extracting all sample with source vocabulary ICD10CM.")

# all data counts after filtering white individual and removing duplicate
sample_num = len(analysis_df)
person_num = analysis_df['person_id'].value_counts()
print("Individual counts after filtering: ", len(person_num), "Sample counts after filtering: ", sample_num)

# count code and num, e.g., A01.03
code_counts = analysis_df['source_concept_code'].value_counts()

# count code and num, e.g., A0103
analysis_df['source_concept_code_children'] = analysis_df['source_concept_code'].astype(str).str.replace('.', '', regex=False)
code_counts_icm10cm = analysis_df['source_concept_code_children'].value_counts()

code_map = analysis_df.drop_duplicates('source_concept_code_children') \
                      .set_index('source_concept_code_children')['source_concept_code']

summary_df = pandas.DataFrame({
    'ICD10_code': code_counts_icm10cm.index, 
    'all_case_count': code_counts_icm10cm.values,
})
summary_df['ICD10_code_original'] = summary_df['ICD10_code'].map(code_map)
summary_df['phecode'] = code_counts.index.map(
    dict(zip(map_ICD10_phecode_df['ICD'], map_ICD10_phecode_df['phecode']))
)
summary_df['ICM10_code_order'] = summary_df['ICD10_code'].map(
    dict(zip(icd10cm_order['code'], icd10cm_order['order']))
)

def count_female(code):
    return len(analysis_df[
        (analysis_df['source_concept_code_children'] == code) &
        (analysis_df['sex_at_birth'] == 'Female')
    ])

def count_male(code):
    return len(analysis_df[
        (analysis_df['source_concept_code_children'] == code) &
        (analysis_df['sex_at_birth'] == 'Male')
    ])

summary_df['description (HIPAA-covered)'] = summary_df['ICD10_code'].map(
    dict(zip(icd10cm_order['code'], icd10cm_order['description']))
)
output_path = os.path.join(os.getcwd(), f"ICD10CM_childrencode.csv")
summary_df.to_csv(output_path, mode='w', index=False)

# get individual id list for top n ICD10 childrencode
n=10000

# check if the dictionary exist
output_dir = os.path.join(os.getcwd(), "european_ancestry_children_individual")
os.makedirs(output_dir, exist_ok=True)
individual_count = []

for i in range(10356, 10357):
    target = summary_df['ICD10_code'].iloc[i]
    target_output_path = os.path.join(output_dir, f"{target}.csv")
    target_ICM10CM = analysis_df[analysis_df['source_concept_code_children'] == target]
    id_counts_target = target_ICM10CM['person_id'].value_counts()
    target_df = pandas.DataFrame({
        'id': id_counts_target.index, 
        'count': id_counts_target.values,
    })
    individual_count.append(target_df.shape[0])
    target_df.to_csv(target_output_path, mode='w', index=False)
    if i % 1000 == 0:
        print("finished 1000 more")

### random select 50000 sample that has WGS data in European ancestry ###
import numpy as np

# extract all ids that have WGS data from reference .fam file
WGS_fam_file = "/home/jupyter/workspaces/controlledtierdatasetv8/v8_plink_data/chr20.fam"
with open(WGS_fam_file, "r") as f:
    wgs_ids = {line.strip().split()[1] for line in f}

# find the intersection of ids with WGS and european ids
analysis_id = {str(id) for id in analysis_df['person_id']}
analysis_wgs_ids = wgs_ids & analysis_id
print("Num of European ancestry with WGS data:", len(analysis_wgs_ids)) 

# random select 50000 samples for intersection list and save their ids
sampleinformation_output_path = os.path.join(os.getcwd(), "european_ancestry_50000sample_id.csv")
np.random.seed(42)
selected_ids = np.random.choice(list(analysis_wgs_ids), size=50000, replace=False)
pd.DataFrame(selected_ids, columns=["person_id"]).to_csv(sampleinformation_output_path, index=False)

print("Num of selected European individuals:", len(selected_ids))
selected_df = analysis_df[analysis_df['person_id'].astype(str).isin(selected_ids)]
print("Num of selected samples:", len(selected_df))

# save information dataframe of selected 50000 individuals
selected_id_path = os.path.join(os.getcwd(), "european_ancestry_50000sample_id.csv")
selected_information_path = os.path.join(os.getcwd(), "european_ancestry_50000sample.csv")
selected_id_df = pd.read_csv(selected_id_path)
selected_information_df = analysis_df[analysis_df["person_id"].astype(str).isin(selected_id_df["person_id"].astype(str))]
selected_information_df.to_csv(selected_information_path, index=False)