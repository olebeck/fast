package fast

import (
	"context"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/valkey-io/valkey-go"
)

type Queue2 struct {
	url    *url.URL
	client valkey.Client
}

func NewQueue2(queueUrl string) (q *Queue2, err error) {
	q = &Queue2{}
	q.url, err = url.Parse(queueUrl)
	if err != nil {
		return nil, err
	}
	err = q.Connect()
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Queue2) Connect() (err error) {
	username := q.url.User.Username()
	password, _ := q.url.User.Password()
	client, err := valkey.NewClient(valkey.ClientOption{
		Username:    username,
		Password:    password,
		InitAddress: []string{q.url.Host},
	})
	q.client = client
	return nil
}

func (q *Queue2) Send(queueName string, body string) error {
	publish := q.client.B().Publish().Channel(queueName).Message(body).Build()
retry:
	res := q.client.Do(context.Background(), publish)
	err := res.Error()
	if err != nil {
		logrus.Error(err)
		time.Sleep(10 * time.Second)
		goto retry
	}
	return err
}

func (q *Queue2) Subscribe(queueName string, process func(messageData []byte)) {
	subscribe := q.client.B().Subscribe().Channel(queueName).Build()
	err := q.client.Receive(context.Background(), subscribe, func(msg valkey.PubSubMessage) {
		process([]byte(msg.Message))
	})
	logrus.Fatal(err)
}

func (q *Queue2) Close() error {
	q.client.Close()
	return nil
}
