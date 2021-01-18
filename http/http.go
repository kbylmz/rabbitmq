package http

import (
	"net/http"

	"github.com/inanzzz/client/internal/pkg/rabbitmq"
	"github.com/inanzzz/client/internal/user"
)

type Router struct {
	*http.ServeMux
}

func NewRouter() *Router {
	return &Router{http.NewServeMux()}
}

func (r *Router) RegisterUsers(rabbitmq *rabbitmq.RabbitMQ) {
	create := user.NewCreate(rabbitmq)

	r.HandleFunc("/users/create", create.Handle)
}