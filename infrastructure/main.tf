provider "aws" {
  region = "us-east-1" 
}

# 1. Create the bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "quantstream-lake-lorenzo-2026"

  # Esto permite borrar el bucket aunque tenga cosas adentro (solo para desarrollo)
  force_destroy = true 

  tags = {
    Name  = "QuantStream Data Lake"
    Owner = "Lorenzo"
  }
}

# restrict public acc
resource "aws_s3_bucket_public_access_block" "public_access" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. folders in a block !
# content is for windows 

resource "aws_s3_object" "bronze" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/"
  content = "" 
}

resource "aws_s3_object" "silver" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "gold/"
  content = ""
}

output "nombre_del_bucket" {
  value = aws_s3_bucket.data_lake.id
}