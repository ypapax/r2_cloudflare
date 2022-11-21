package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"log"
	"os"
)

//https://developers.cloudflare.com/r2/examples/aws-sdk-go/

func getenv(k string) string {
	v := os.Getenv(k)
	log.Printf("env %+v=%+v", k, v)
	return v
}

func main() {
	ctx := context.Background()
	log.SetFlags(log.LstdFlags | log.Llongfile)
	var bucketName = getenv("CLOUDFLARE_R2_BUCKET_NAME")
	var accessKeyId = getenv("CLOUDFLARE_R2_Access_Key_ID")
	var accessKeySecret = getenv("CLOUDFLARE_R2_Secret_Access_Key")
	var s3api = getenv("CLOUDFLARE_R2_S3_API")

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: s3api,
		}, nil
	})

	if err := func() error {
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithEndpointResolverWithOptions(r2Resolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
		)
		if err != nil {
			log.Fatal(err)
		}

		client := s3.NewFromConfig(cfg)
		cbo, errC := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketName})
		if errC != nil {
			return errors.WithStack(errC)
		}
		log.Printf("cbo.ResultMetadata: %+v", cbo.ResultMetadata)
		listObjectsOutput, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucketName,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("len(listObjectsOutput.Contents): %+v", listObjectsOutput.Contents)
		for _, object := range listObjectsOutput.Contents {
			obj, _ := json.MarshalIndent(object, "", "\t")
			fmt.Println(string(obj))
		}

		//  {
		//  	"ChecksumAlgorithm": null,
		//  	"ETag": "\"eb2b891dc67b81755d2b726d9110af16\"",
		//  	"Key": "ferriswasm.png",
		//  	"LastModified": "2022-05-18T17:20:21.67Z",
		//  	"Owner": null,
		//  	"Size": 87671,
		//  	"StorageClass": "STANDARD"
		//  }

		listBucketsOutput, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		if err != nil {
			log.Fatal(err)
		}

		for _, object := range listBucketsOutput.Buckets {
			obj, _ := json.MarshalIndent(object, "", "\t")
			fmt.Println(string(obj))
		}
		return nil
	}(); err != nil {
		log.Printf("error: %+v", err)
	} else {
		log.Printf("ok")
	}


}
