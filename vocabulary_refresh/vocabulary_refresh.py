'''
    Refresh OMOP Vocabularies and Custom Concepts
'''

# ----------------------------------------------------
'''
These tables are kept after clean-up:
    tmp_custom_mapping - the source table loaded from CSV files
    tmp_custom_concept_skipped 
        - output table with tmp_custom_mapping rows which were not processed because of errors

Known issue:
    need to stop if the previous step failed

'''
# ----------------------------------------------------

import os
import sys
import getopt
import json

# read params
# - what step does the user want to run
def read_params():

    params_description = {
        "step": "mandatory: indicate '-s' for 'step', integer step number" ,
        "config_file":   "optional: indicate '-c' for 'config', default is 'vocabulary_refresh.conf'"
    }
    params = {}

    step_values = {
        "0": "run all steps",

        "10": "run steps 11-13 for athena standard vocabularies",
        "11": "copy files to gs",
        "12": "load from gs to intermediate bq tables tmp_*",
        "13": "create final voc tables from intermediate",

        "20": "run steps 21-23 for custom mapping",
        "21": "copy files to gs",
        "22": "load from gs to intermediate bq table tmp_custom_mapping, verify the table",
        "23": "create custom concepts and its relationships in voc tables",

        "30": "run steps 31-32 for vocabulary tables",
        "31": "check vocabulary tables",
        "32": "clean up empty check tables"
    }
    
    print('Read params...')

    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"s:c:",["step=", "config="])

        # default 값으로 추정.
        params = {
            "step": -10,
            "config_file": "vocabulary_refresh.conf"
        }

        for opt, arg in opts:
            if opt in ['-s', '--step']:
                params['step'] = int(arg)
            if opt in ['-c', '--config']:
                params['config_file'] = arg

        if params['step'] < 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
        if not os.path.isfile(params['config_file']):
            raise getopt.GetoptError("read_params() error", "Config file '{f}' is not found".format(
                f=params['config_file']))

    except getopt.GetoptError as err:
        print(params)
        print(err.args)
        print("Please indicate correct params:")
        print(params_description)
        for k in sorted(step_values.keys()):
            s = "    {k} - {v}".format(k=k, v=step_values[k])
            print(s)
        sys.exit(2)

    print(params)
    return params

# read paths and target vocabulary dataset name
def read_config(config_file):

    print('Read config file...')
    config = {}

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config = json.load(f)

    print(config_file)
    return config

# ----------------------------------------------------
# const
# ----------------------------------------------------

#####To refresh standard vocabularies#####

# ----------------------------------------------------
# main
# ----------------------------------------------------

# 이 파일 이름이 Refresh인 이유는 `Standardized Vocabularies` 스키마의 테이블을 최신화된 버전으로 다시 테이블을 replace하기 때문이다.
# 진행하기에 앞서, vocabulary_refresh/Readme.md 에 따르면 ATHENA에서 내려받은 표준어휘집을 

# Workflow:
#
# 1. Athena로부터 어휘집 다운받기
# 2. gsutil에 올라간 기존 어휘집 삭제 후, 다운받은 어휘집 업로드 하기.
# 3. gsutil에 올라간 어휘집을 기반으로 BigQuery를 수행.
#    tmp_* 테이블 생성. (e.g., tmp_concept, tmp_concept_ancestor, etc.)
#    즉, 여기서는 스냅샷을 찍는 역할을 수행한다.
# 4. 만들어진 tmp_* 테이블에서 `create_voc_from_tmp.sql`을 실행해서 tmp_concept -> concept 테이블로 바꾸는 식으로 쿼리 진행.
# 5. custom mapping 파일을 정의하고(vocabulary_refresh/custom_mapping_template 내용 참고.), custom_mapping_list.tsv도 업데이트한다.
# 6. custom mapping 파일을 gsutil에 올리기
# 7. tmp_custom_mapping 이라는 테이블을 정의하고 `gs_path`경로에 있는 모든 csv 파일들(i.e., custom_mapping_csv/*.csv)을 넣는다.
# 8. tmp_custom_mapping 으로부터 각각의 임시 테이블을 생성하고(e.g., tmp_custom_concept, tmp_custom_vocabulary, etc.), 
#    기존 어휘집 테이블 중 업데이트가 될 테이블만 임시 테이블을 생성하며(e.g., tmp_voc_concept, tmp_voc_concept_relationship, tmp_voc_vocabulary),
#    그 임시 테이블에 새로운 매핑의 임시 테이블 데이터를 업데이트(INSERT) 한다.
#    그 후 이것을 새로운 어휘집 테이블로 생성한다.

def main():

    gsutil_rm_csv = "gsutil rm {target_path}/*.csv"
    gsutil_cp_csv = "gsutil cp {source_path}/*.csv {target_path}/"
    dataset_fullname = "`{project}.{dataset}`"
    run_command_load = "python load_to_bq_vocab.py --{step_name} --config {config_file}"
    # run_command_bq_script = "python bq_run_script.py --config {config_file} {script_file}"
    run_command_bq_script = "python bq_run_script.py -c {config_file} {script_file}"

    # 여기서부터 시작. 
    # `python vocabulary_refresh.py -s10`
    # params = {'step': 10, 'config': 'vocabulary_refresh.conf'}
    params = read_params() 
    config = read_config(params['config_file'])

    return_code = 0

    # 1. Download and unzip files from Athena
    # We start from from the point when Athena vocabularies are downloaded and CPT vocabulary is updated

    # 2. Copy files to GCP bucket
    if return_code == 0 and params['step'] in [11, 10, 0]:
        # `gsutil rm gs://mimic_iv_to_omop/[임의의 위치]/*.csv` 를 실행해 파일을 삭제한다.
        run_command = gsutil_rm_csv.format(target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

        # `gsutil cp [다운받은 ATHENA 표준어휘집 로컬 위치]/*.csv gs://mimic_iv_to_omop/[임의의 위치]/`를 실행해 
        # 현재 프로젝트 내에 있는 `[다운받은 ATHENA 표준어휘집 로컬 위치]`의 파일들을 google storage의 `gs://mimic_iv_to_omop/[임의의 위치]/`로 복사하여 올린다.
        run_command = gsutil_cp_csv.format(
            source_path=config['local_athena_csv_path'], target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 3. Load files to intermediate BQ tables
    if return_code == 0 and params['step'] in [12, 10, 0]:
        # `python load_to_bq_vocab.py --athena --config vocabulary_refresh.conf`
        # `Standardized Vocabularies` 스키마의 테이블들을 `vocabulary_refresh.conf`에 지정된 `gs_athena_csv_path`로 csv파일 형식으로 로딩한다.
        # `run_command`는 커맨드에서 로딩에 성공하지 못한 테이블을 16진수 형식으로 보여준다.
        run_command = run_command_load.format(step_name="athena", config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 4. Populate target vocabulary tables from intermediate tables
    # 모든 테이블이 잘 로드되었고 step이 지정되었다면 실행한다.
    if return_code == 0 and params['step'] in [13, 10, 0]:
        # `python bq_run_script.py -c vocabulary_refresh.conf create_voc_from_tmp.sql`
        # configure가 준비되면 `create_voc_from_tmp.sql` 스크립트를 실행한다.
        # `Standardized Vocabularies` 스키마의 특정 version을 가져와 테이블들이 모두 create 및 replace된다.
        # 테이블 중에서 사용자 지정 개념에 영향을 받는 테이블은 다음과 같다: `concept`, `concept_relationship`, `vocabulary` TABLE.
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="create_voc_from_tmp.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)


    # 위까지는 기본적인 DB내 테이블 만들기 작업이었다. 
    # 그 말인즉슨 위 과정은 CSV만 있다면 postgresql에 manual하게 올릴 수 있다는 뜻이다.


    #####To refresh or add new custom mapping#####

    # 5. Copy custom mapping files to custom_mapping_csv/ folder, and update custom_mapping_list.tsv
    # step 11(process 2)을 사용해서 `gs://mimic_iv_to_omop/custom_mapping_csv/custom_mapping_list.tsv`를 업데이트 하라는 것으로 보임.
    # It is a manual step

    # 6. Copy custom mapping files to GCP bucket
    if return_code == 0 and params['step'] in [21, 20, 0]:
        # `gsutil rm gs://mimic_iv_to_omop/custom_mapping/*.csv` 를 실행해 gs의 `custom_mapping` 디렉터리 내 csv파일들을 모두 삭제한다.
        run_command = gsutil_rm_csv.format(target_path=config['gs_mapping_csv_path'])
        print(run_command)

        # `gsutil cp ../custom_mapping_csv/*.csv gs://mimic_iv_to_omop/custom_mapping` 을 실행해
        # 현재 프로젝트 내에 있는 `/custom_mapping_csv` 디렉터리의 하위 tsv, csv 파일들을 gs로 복사하여 올린다.
        # 즉, 우리가 이후 타병원과의 협업을 진행한다면, 본 프로젝트의 하위 디렉터리인 `custom_mapping_csv`에 해당 mapping을
        # 다른 파일들의 양식을 참조하여 올리면 된다는 뜻이다.
        #
        # 현재 `custom_mapping_csv` 디렉터리에 gcpt가 있는 이유는 이것이 ATHENA에서 default selection vocab에 해당하지만
        # 별도의 EULA라는 별도의 인증을 요구하여 다른 사용자들은 접근에 어려움이 있기 때문이다.
        run_command = gsutil_cp_csv.format(
            source_path=config['local_mapping_csv_path'], target_path=config['gs_mapping_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 7. Load files to the intermediate BQ table (tmp_custom_mapping)
    # 사용자 지정 매핑 테이블만 지정된 `bq_target_dataset`의 `schemas_dir_all_csv`에 테이블로 불러온다.
    if return_code == 0 and params['step'] in [22, 20, 0]:
        run_command = run_command_load.format(step_name="mapping", config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="check_custom_loaded.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 8. Add custom concepts to vocabulary tables from the intermediate table
    # 로딩된 사용자 지정 매핑 테이블의 사용자 지정 개념을 어휘 테이블에 추가하기
    # `load_to_bq_vocab.py`의 `load_table` 함수에서 `tmp_custom_mapping` 테이블(intermediate table)을 만들고
    # 이것으로부터 여러 테이블이 파생되며 최종적으로 기존 OMOP CDM 테이블을 Replace하게 된다.
    # 자세한 내용은 `custom_vocabularies.sql`의 `Data flow`를 참고하라.
    # 
    # 결론적으로, Custom concept, vocabulary, relationship이 반영된 새로운 CDM이 만들어진다.
    if return_code == 0 and params['step'] in [23, 20, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="custom_vocabularies.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    #####Verify the result#####

    # 9. Verify target tables
    if return_code == 0 and params['step'] in [31, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_check_bq.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 10. Clean up temporary tables from the previous step
    # collect the list to clean up
    if return_code == 0 and params['step'] in [32, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_cleanup_bq_m.sql") # remove only empty check tables
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

# ----------------------------------------------------
# go
# ----------------------------------------------------
main()

exit(0)
