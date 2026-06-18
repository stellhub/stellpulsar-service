package main

import (
	"log"

	"github.com/stellhub/stellar"
	"github.com/stellhub/stellpulsar-service/internal/app"
)

func main() {
	if err := stellar.Run(stellar.WithStarter(app.NewStarter())); err != nil {
		log.Fatal(err)
	}
}
