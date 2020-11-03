python code/KBQA_Runner.py  \
        --train_folder  data/train_SQ \
        --dev_folder data/dev_SQ \
        --test_folder data/test_SQ \
        --vocab_file data/SQ/vocab.txt \
	--KB_file data/SQ/kb_cache.json \
        --M2N_file data/SQ/m2n_cache.json \
        --QUERY_file data/SQ/query_cache.json \
        --output_dir trained_model/SQ \
        --config config/bert_config.json \
        --gpu_id 2\
        --save_model Best \
        --max_hop_num 1 \
        --num_train_epochs 100 \
        --do_train 1 \
        --do_eval 1\
        --do_policy_gradient 1\
        --learning_rate 1e-5 \
        --train_limit_number 150 \