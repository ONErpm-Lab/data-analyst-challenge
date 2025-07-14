SELECT
  store,
  DATE(date) AS date,
  product,
  quantity,
  is_stream,
  is_download,
  revenue,
  currency,
  country_code,
  genre_id,
  genre_name,
  origem_arquivo
FROM {{ source('onerm_dataset', 'tb_dados_one_rpm') }}
