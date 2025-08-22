-- Step 1: Create the OpenAI summarization model
CREATE MODEL `flink-test`.`wonder_cement`.`document_summarizer`
INPUT (`text` VARCHAR(2147483647))
OUTPUT (`summary` VARCHAR(2147483647))
COMMENT 'Document summarization model for RAG applications'
WITH (
  'openai.connection' = 'openai-connection',
  'openai.model_version' = 'gpt-4o-mini',
  'openai.system_prompt' = 'You are an expert document summarizer. Create a comprehensive summary that captures all major details, key concepts, technical terms, and important information from the provided document. The summary should be concise but complete, preserving essential facts, procedures, examples, and critical points. Maintain technical accuracy and include relevant terminology that would be useful for retrieval-augmented generation (RAG) applications. Focus on actionable information, key insights, and important details that users might search for.',
  'provider' = 'openai',
  'task' = 'text_generation'
)

CREATE MODEL `flink-test`.`wonder_cement`.`openai_embedding_model`
INPUT (`text` VARCHAR(2147483647))
OUTPUT (`response` ARRAY<FLOAT>)
WITH (
  'openai.connection' = 'openai-connection-for-embeddings',
  'provider' = 'openai',
  'task' = 'embedding'
)

CREATE TABLE document_with_summary_embeddings AS
SELECT 
  r.document_id,
  r.url,
  r.document_content,
  s.summary AS document_summary,
  e.response AS document_summary_embedding,
  r.`timestamp`,
  CURRENT_TIMESTAMP AS processing_timestamp
FROM url_markdown_raw r,
LATERAL TABLE(ML_PREDICT('document_summarizer', r.document_content)) s,
LATERAL TABLE(ML_PREDICT('`flink-test`.`wonder_cement`.`openai_embedding_model`', s.summary)) e;