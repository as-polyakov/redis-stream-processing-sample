package domain

type Order struct {
	OrderId      string
	CustomerId   string
	CustomerType string
	Store        string
	Price        float32
}

type ClientDeal struct {
	OrderId       string
	CustomerId    string
	CustomerName  string
	CustomerType  string
	Store         string
	TotalOrderSum float32
}
