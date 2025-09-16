# DCL-llm4scvd

This is code and dataset respository for the academic paper "Decoupled Contrastive Learning with LLMs for Long-Tailed Smart Contract Vulnerability Detection".

# this is the commands for supervised finetuning 
CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 \
swift sft \
    --model Qwen/Qwen3-8B \
    --train_type lora \
    --dataset 'AI-ModelScope/alpaca-gpt4-data-zh#500' \
              'AI-ModelScope/alpaca-gpt4-data-en#500' \
              'swift/self-cognition#500' \
    --torch_dtype bfloat16 \
    --num_train_epochs 1 \
    --per_device_train_batch_size 1 \
    --per_device_eval_batch_size 1 \
    --learning_rate 1e-4 \
    --lora_rank 32 \
    --lora_alpha 64 \
    --target_modules all-linear \
    --gradient_accumulation_steps 16 \
    --eval_steps 50 \
    --save_steps 50 \
    --save_total_limit 2 \
    --logging_steps 5 \
    --max_length 2048 \
    --output_dir output \
    --system "You are “SC-Embed,” a Solidity representation expert. Your job is to read and understand a smart contract and can learn multi-granularity Representatio that captures features most predictive of behavior, security, and standards conformance. " \
    --warmup_ratio 0.05 \
    --dataloader_num_workers 4 \
    --model_author swift \
    --model_name swift-robot
