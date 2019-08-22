package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"time"

	"gopkg.in/olivere/elastic.v7"
)

// format of messages sent to ElasticSearch
type MyType struct {
	Id      int       `json:"id"`
	Time    time.Time `json:"@timestamp"`
	Message string    `json:"message"`
}

func main() {

	ctx := context.Background()

	// load command line arguments
	server := flag.String("server", "http://localhost:9200", "ElasticSearch server e.g. http://localhost:9200")
	flag.Parse()

	// configure connection to ES
	client, err := elastic.NewClient(elastic.SetURL(*server))
	if err != nil {
		panic(err)
	}
	log.Printf("client.running? %v", client.IsRunning())
	if !client.IsRunning() {
		panic("Could not make connection, not running")
	}

	// check ElasticSearch version
	log.Println("-------ElasticSearch version--------")
	version, verr := client.ElasticsearchVersion(*server)
	if verr != nil {
		panic(verr)
	}
	// make sure this version of API is suited to ES backend
	log.Printf("olivere/elastic API version: %s", elastic.Version)
	fmt.Println(elastic.Version)
	log.Printf("ElasticSearch server version: %s", version)
	if version[0:2] != elastic.Version[0:2] {
		panic(fmt.Sprintf("This API oliver/elastic version are not maching, please Import 'gopkg.in/olivere/elastic.%s' ", version))
	}

	log.Println("-------ElasticSearch insert--------")
	// insert row of data into index=myindex, type=mytype
	row := MyType{
		Time: time.Now(),
		//Message: fmt.Sprintf("message inserted at %s", time.Now()),
		Id:      15,
		Message: "message",
	}
	ires, ierr := client.Index().Index("myindex1").Type("mytype").BodyJson(row).Refresh("true").Do(ctx)
	if ierr != nil {
		panic(ierr)
	}
	log.Printf("Successfully inserted row of data into myindex/mydata: %+v", ires)

	log.Println("-------ElasticSearch search--------")
	queryID := elastic.NewRangeQuery("id").Gte(10)

	// termQuery := elastic.NewTermQuery("message", "message")

	genralQ := elastic.NewBoolQuery().Should().Filter(queryID)

	// Do a search
	//searchResult, err := client.Search().Index("myindex").Query(termQuery).Do(context.Background())

	res, err := client.Search().Index("myindex1").Query(genralQ).Do(context.Background())

	if err != nil {
		panic(err)
	}
	fmt.Printf("Query took %d milliseconds\n", res.TookInMillis)

	fmt.Println("Rows found:")
	var l MyType
	for _, item := range res.Each(reflect.TypeOf(l)) {
		l := item.(MyType)
		fmt.Printf("time: %s Id: %d  message: %s\n ", l.Time, l.Id, l.Message)
	}
	log.Println("done with search")

}
