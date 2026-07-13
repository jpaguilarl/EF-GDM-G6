# 1. Variables
export BUCKET=tlc-gdm-ef-2026
export BASE=tlc-pipeline/data/silver/star/facts/fact_fhvhv_trip
export MESES="02 03 04 05 06"

# 2. Estado de cada mes
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  if aws s3 ls "s3://$BUCKET/$BASE/2024-$m.parquet/_SUCCESS" >/dev/null 2>&1; then
    echo "2024-$m  OK"
  elif aws s3 ls "s3://$BUCKET/$BASE/2024-$m.parquet/" >/dev/null 2>&1; then
    echo "2024-$m  SOSPECHOSO (sin _SUCCESS)"
  else
    echo "2024-$m  ausente"
  fi
done

# 3. Detectar restos de commits abortados
aws s3 ls "s3://$BUCKET/$BASE/" --recursive | grep -E "/_temporary/|/__magic" || echo "sin restos"
aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$BASE/" --query 'Uploads[].[Key,UploadId,Initiated]' --output table

# 4. Borrar meses sospechosos completos
for m in $MESES; do
  aws s3 rm "s3://$BUCKET/$BASE/2024-$m.parquet/" --recursive
done

# 5. Borrar restos _temporary huérfanos
aws s3 rm "s3://$BUCKET/$BASE/" --recursive --exclude "*" --include "*/_temporary/*"

# 6. Abortar multipart uploads colgados
aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$BASE/" --query 'Uploads[].[Key,UploadId]' --output text | while read -r KEY UID; do
  [ -n "$KEY" ] && aws s3api abort-multipart-upload --bucket "$BUCKET" --key "$KEY" --upload-id "$UID"
done

# 7. Confirmar que quedó limpio
aws s3 ls "s3://$BUCKET/$BASE/" --recursive | grep -E "/_temporary/|/__magic" || echo "limpio"
aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$BASE/" --query 'length(Uploads)' --output text
for m in $MESES; do
  aws s3 ls "s3://$BUCKET/$BASE/2024-$m.parquet/" >/dev/null 2>&1 && echo "2024-$m EXISTE" || echo "2024-$m borrado"
done

# 8. Verificar _SUCCESS (tamaño > 0) tras reescribir
for m in $MESES; do
  echo -n "2024-$m: "
  aws s3 ls "s3://$BUCKET/$BASE/2024-$m.parquet/_SUCCESS" | awk '{print $3" bytes"}' || echo "SIN _SUCCESS"
done


# 9. Ver el manifest (confirma committer magic)
aws s3 cp "s3://$BUCKET/$BASE/2024-02.parquet/_SUCCESS" - | head -c 2000

# 10. Solo part-files válidos, sin basura
aws s3 ls "s3://$BUCKET/$BASE/2024-02.parquet/" | grep -E "part-.*\.parquet|_SUCCESS"


# 11. Verificación final: sin restos y sin multipart
aws s3 ls "s3://$BUCKET/$BASE/" --recursive | grep -E "/_temporary/|/__magic" || echo "limpio"
aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$BASE/" --query 'length(Uploads)' --output text

