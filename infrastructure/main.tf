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

resource "aws_glue_catalog_database" "quantstream_db" {
  name = "quantstream_db"
}

#crawler robot
resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.quantstream_db.name
  name          = "quantstream_silver_crawler"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/silver/crypto/binance/ticker/"
  }

  tags = {
    Project = "QuantStream"
  }
}

#IAM Role. Read and write perms
resource "aws_iam_role" "glue_crawler_role" {
  name = "quantstream_glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# ("AWSGlueServiceRole")
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# perms for my bucket
resource "aws_iam_policy" "glue_s3_policy" {
  name = "quantstream_glue_s3_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject"]
      Resource = [
        "${aws_s3_bucket.data_lake.arn}/silver/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}