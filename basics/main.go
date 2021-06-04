// Following Quick Start tutorial from https://www.mongodb.com/blog/search/golang%20quickstart
//
// 1) Starting and setup
// 2) How to Create Documents
// 3) How to Read Documents
// 4) How to Update Documents
// 5) How to Delete Documents
//
package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	// 1) Setup of DB
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Create a context...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		log.Println("Cancel Context...")
		cancel()
	}()

	log.Println("Connect to MongoDB...")
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		log.Println("Disconnect from MongoDB...")
		client.Disconnect(ctx)
	}()

	log.Println("Ping the database")
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	log.Println("List the databases")
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Available databases:", databases)

	// 2) Insert Documents
	quickstartDB := client.Database("quickstart")
	podcastsCollection := quickstartDB.Collection("podcasts")
	episodesCollection := quickstartDB.Collection("episodes")

	_, err = podcastsCollection.InsertOne(ctx, bson.D{
		{Key: "title", Value: "The Polyglot Dev Pod"},
		{Key: "author", Value: "Nic Raboy"},
	})
	if err != nil {
		log.Fatal(err)
	}

	podcastId, err := podcastsCollection.InsertOne(ctx, bson.D{
		{Key: "title", Value: "The Polyglot Dev Pod"},
		{Key: "author", Value: "Nic Raboy"},
		{Key: "tags", Value: bson.A{"development", "programming", "coding"}},
	})
	if err != nil {
		log.Fatal(err)
	}

	episodeRes, err := episodesCollection.InsertMany(ctx, []interface{}{
		bson.D{
			{"podcast", podcastId.InsertedID},
			{"title", "GraphQL..."},
			{"descriptions", "Foo bar"},
			{"duration", 25},
		},
		bson.D{
			{"podcast", podcastId.InsertedID},
			{"title", "Prog Web..."},
			{"descriptions", "Alpha beta"},
			{"duration", 32},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Inserted %v docs into episode collection!\n", len(episodeRes.InsertedIDs))

	// 3a) Read all documents into a slice episodes (Dangerous for large datasets)
	cursor, err := episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	var episodes []bson.M
	err = cursor.All(ctx, &episodes)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Episodes read:", episodes)

	// 3b) Read documents one at a time, using the cursor
	cursor, err = episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(ctx)
	for i := 1; cursor.Next(ctx); i++ {
		var episode bson.M
		err := cursor.Decode(&episode)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Doc #%d: %v", i, episode)
	}

	// 3c) Read a single document
	var podcast bson.M
	err = podcastsCollection.FindOne(ctx, bson.M{}).Decode(&podcast)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Read single podcast:", podcast)

	// 3d) Read with a filter (duration == 25)
	filterCursor, err := episodesCollection.Find(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Fatal(err)
	}
	var episodesFiltered []bson.M
	err = filterCursor.All(ctx, &episodesFiltered)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Filtered episodes:", episodesFiltered)

	// 3e) Find with sort and filter (duration > 20)
	opts := options.Find().SetSort(bson.D{{"duration", -1}})
	sortCursor, err := episodesCollection.Find(ctx, bson.D{{"duration", bson.D{{"$gt", 20}}}}, opts)
	if err != nil {
		log.Fatal(err)
	}

	var episodesSorted []bson.M
	err = sortCursor.All(ctx, &episodesSorted)
	if err != nil {
		log.Fatal(err)
	}
	for i, episode := range episodesSorted {
		log.Printf("Sorted Doc #%d: %v", i+1, episode)
	}

	// 4a) Update a document
	var podcasts []bson.M
	podCursor, _ := podcastsCollection.Find(ctx, bson.M{"_id": podcastId.InsertedID})
	podCursor.All(ctx, &podcasts)
	log.Println("Author Before Update:", podcasts[0]["author"])
	updateOneResult, err := podcastsCollection.UpdateOne(
		ctx,
		bson.M{"_id": podcastId.InsertedID},
		bson.D{
			{"$set", bson.D{{"author", "Nicky Raboy"}}},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(podcastId)
	log.Printf("Modified %v documents!\n", updateOneResult.ModifiedCount)
	podCursor, _ = podcastsCollection.Find(ctx, bson.M{"_id": podcastId.InsertedID})
	podCursor.All(ctx, &podcasts)
	log.Println("Author After Update:", podcasts[0]["author"])

	// 4b) Update Many
	result, err := podcastsCollection.UpdateMany(
		ctx,
		bson.M{"title": "The Polyglot Dev Pod"},
		bson.D{
			{"$set", bson.D{{"author", "Nicolas Raboy"}}},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Updated %v Documents!\n", result.ModifiedCount)

	// 4c) ReplaceOne
	result, err = podcastsCollection.ReplaceOne(
		ctx,
		bson.M{"author": "Nicolas Raboy"},
		bson.M{
			"title":  "The Nic Raboy Show",
			"author": "Nico Raboy",
		},
	)
	log.Printf("Replaced %v Documents!\n", result.ModifiedCount)

	// 5a) Delete One Document
	deletedRes, err := podcastsCollection.DeleteOne(ctx, bson.M{"_id": podcastId.InsertedID})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Number of deleted docs: %v\n", deletedRes.DeletedCount)

	// 5b) Delete Many Documents
	deletedRes, err = episodesCollection.DeleteMany(ctx, bson.M{"duration": 25})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Number of deleted docs: %v\n", deletedRes.DeletedCount)

	// 5c) Drop collections
	err = podcastsCollection.Drop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Dropped podcasts collection")

	err = episodesCollection.Drop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Dropped episodes collection")

	err = quickstartDB.Drop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Dropped quickstart database")
}
