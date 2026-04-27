import asyncio
import os
from raganything import RAGAnything
from lightrag import LightRAG
from lightrag.kg.shared_storage import initialize_pipeline_status
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.utils import EmbeddingFunc

async def main():
    print("="*50)
    print("🚀 正在唤醒 RAG-Anything 企业知识大脑...")
    print("="*50)
    
    # 1. 填入你的真实 API Key (或者通过 os.getenv 读取)
    DASHSCOPE_API_KEY = "sk-0f42184c07864da6ae6a04dfedb76630"  # 替换为你的百炼 Key
    SILICONFLOW_API_KEY = "sk-tsajrtswhzasgltriqttqfqpnicnmqsvrulttmjnlrguzeiq"  # 替换为你的硅基流动 Key

    # 2. 配置与入库时完全相同的模型函数
    def llm_model_func(prompt, system_prompt=None, history_messages=[], **kwargs):
        return openai_complete_if_cache(
            "qwen3.5-plus-2026-02-15", prompt, system_prompt=system_prompt,
            history_messages=history_messages, api_key=DASHSCOPE_API_KEY,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1", **kwargs
        )

    embedding_func = EmbeddingFunc(
        embedding_dim=1024, max_token_size=8192,
        func=lambda texts: openai_embed.func(
            texts, model="BAAI/bge-m3", api_key=SILICONFLOW_API_KEY,
            base_url="https://api.siliconflow.cn/v1"
        )
    )

    # 3. 核心修复：显式加载已存在的底层 LightRAG 图谱库
    print("📦 正在加载底层知识图谱与向量存储...")
    lightrag_instance = LightRAG(
        working_dir="./rag_workspace",
        llm_model_func=llm_model_func,
        embedding_func=embedding_func
    )
    
    # 关键步骤：激活存储状态
    await lightrag_instance.initialize_storages()
    await initialize_pipeline_status()

    # 4. 将激活的底层引擎挂载给 RAG-Anything
    rag = RAGAnything(lightrag=lightrag_instance)
    
    print("\n✅ 知识大脑已就绪！(输入 'exit' 或 'quit' 退出)")
    
    # 5. 开启无限问答循环
    while True:
        query = input("\n🧑‍💻 请提问: ")
        if query.lower() in ['exit', 'quit']:
            break
        if not query.strip():
            continue
            
        print("🤖 正在思考与检索中，请稍候...")
        # 使用 hybrid 模式：融合向量相似度与图谱逻辑
        result = await rag.aquery(query, mode="hybrid")
        
        print("\n" + "-"*50)
        print("💡 回答：\n")
        print(result)
        print("-"*50)

if __name__ == "__main__":
    asyncio.run(main())