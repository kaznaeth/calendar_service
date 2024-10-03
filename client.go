package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yourusername/calendar_service/proto/calendar"
    "github.com/golang-jwt/jwt/v5"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
)

var jwtSecret = []byte("your_secret_key")

func main() {
    // Устанавливаем соединение с сервером
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := calendar.NewCalendarServiceClient(conn)

    // Генерируем JWT токен
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "user": "test_user",
        "exp":  time.Now().Add(time.Hour * 1).Unix(),
    })
    tokenString, err := token.SignedString(jwtSecret)
    if err != nil {
        log.Fatalf("Failed to generate token: %v", err)
    }

    // Создаем метаданные с токеном
    md := metadata.New(map[string]string{
        "authorization": tokenString,
    })
    ctx := metadata.NewOutgoingContext(context.Background(), md)

    // Отправляем запрос
    req := &calendar.UploadCalendarRequest{
        WarehouseName:  "Warehouse1",
        AcceptanceType: "TypeA",
        Date:           timestampNow(),
        Coefficient:    1.23,
    }

    res, err := client.UploadCalendar(ctx, req)
    if err != nil {
        log.Fatalf("Error when calling UploadCalendar: %v", err)
    }

    fmt.Println("Response from server:", res.Message)
}

// Helper function to get current timestamp
func timestampNow() *calendar.Timestamp {
    t := time.Now()
    return &calendar.Timestamp{
        Seconds: t.Unix(),
        Nanos:   int32(t.Nanosecond()),
    }
}
