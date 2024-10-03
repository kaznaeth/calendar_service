package main

import (
    "context"
    "log"
    "net"
    "os"
    "time"

    "github.com/yourusername/calendar_service/proto/calendar"
    "github.com/golang-jwt/jwt/v5"
    "github.com/go-redis/redis/v8"
    clickhouse "github.com/ClickHouse/clickhouse-go/v2"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
    "google.golang.org/grpc/metadata"
)

// Secret key for JWT (в реальном приложении храните в переменных окружения)
var jwtSecret = []byte("your_secret_key")

// Redis client
var rdb *redis.Client

// ClickHouse session
var ckConn clickhouse.Conn

type server struct {
    calendar.UnimplementedCalendarServiceServer
}

// JWT Auth Interceptor
func authInterceptor(ctx context.Context) (context.Context, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, grpc.Errorf(grpc.Code(grpc.Unauthenticated), "metadata is not provided")
    }

    tokenString := md["authorization"]
    if len(tokenString) == 0 {
        return nil, grpc.Errorf(grpc.Code(grpc.Unauthenticated), "authorization token is not provided")
    }

    // Parse JWT token
    token, err := jwt.Parse(tokenString[0], func(token *jwt.Token) (interface{}, error) {
        // Проверяем алгоритм
        if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
            return nil, grpc.Errorf(grpc.Code(grpc.Unauthenticated), "unexpected signing method")
        }
        return jwtSecret, nil
    })

    if err != nil || !token.Valid {
        return nil, grpc.Errorf(grpc.Code(grpc.Unauthenticated), "invalid token: %v", err)
    }

    return ctx, nil
}

// Implement the UploadCalendar RPC method
func (s *server) UploadCalendar(ctx context.Context, req *calendar.UploadCalendarRequest) (*calendar.UploadCalendarResponse, error) {
    // Проверка аутентификации
    ctx, err := authInterceptor(ctx)
    if err != nil {
        return nil, err
    }

    // Генерируем ключ для Redis
    redisKey := req.WarehouseName + ":" + req.AcceptanceType + ":" + req.Date.AsTime().Format("2006-01-02")

    // Проверяем, есть ли изменение данных по хешу
    // Здесь можно использовать JSON представление данных или уникальное значение
    // Для простоты возьмем коэффициент как проверку

    rdbValue, err := rdb.Get(ctx, redisKey).Result()
    if err == redis.Nil || rdbValue != req.Coefficient {
        // Обновляем в Redis
        err := rdb.Set(ctx, redisKey, req.Coefficient, 0).Err()
        if err != nil {
            return nil, err
        }

        // Сохраняем в ClickHouse
        batch, err := ckConn.PrepareBatch(ctx, "INSERT INTO calendar (warehouse_name, acceptance_type, date, coefficient)")
        if err != nil {
            return nil, err
        }

        err = batch.Append(
            req.WarehouseName,
            req.AcceptanceType,
            req.Date.AsTime(),
            req.Coefficient,
        )
        if err != nil {
            return nil, err
        }

        err = batch.Send()
        if err != nil {
            return nil, err
        }
    } else {
        // Данные не изменились
        log.Println("Data is unchanged, skipping update")
    }

    return &calendar.UploadCalendarResponse{
        Message: "Calendar updated successfully",
    }, nil
}

func main() {
    // Инициализация Redis
    rdb = redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
        // Password: "", // При необходимости
        DB: 0, // Используем 0 базу данных
    })

    // Тестовое подключение к Redis
    _, err := rdb.Ping(context.Background()).Result()
    if err != nil {
        log.Fatalf("Could not connect to Redis: %v", err)
    }

    // Инициализация ClickHouse
    ckConn, err = clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            // Username: "", // При необходимости
            // Password: "", // При необходимости
        },
        DialTimeout:  5 * time.Second,
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
    })

    if err != nil {
        log.Fatalf("Could not connect to ClickHouse: %v", err)
    }

    // Создаем таблицу, если не существует
    ctx := context.Background()
    err = ckConn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS calendar (
            warehouse_name String,
            acceptance_type String,
            date DateTime,
            coefficient Float64
        ) ENGINE = MergeTree()
        ORDER BY (warehouse_name, acceptance_type, date)
    `)
    if err != nil {
        log.Fatalf("Could not create table in ClickHouse: %v", err)
    }

    // Настраиваем gRPC сервер
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("Failed to listen: %v", err)
    }

    // Опционально: настроить SSL/TLS
    var opts []grpc.ServerOption
    // creds, err := credentials.NewServerTLSFromFile("server.crt", "server.key")
    // if err != nil {
    //     log.Fatalf("Failed to load TLS credentials: %v", err)
    // }
    // opts = append(opts, grpc.Creds(creds))

    grpcServer := grpc.NewServer(opts...)
    calendar.RegisterCalendarServiceServer(grpcServer, &server{})

    log.Println("Server is running on port :50051")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("Failed to serve: %v", err)
    }
}
