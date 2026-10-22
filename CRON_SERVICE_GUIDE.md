# Leader Election для Крон-Сервиса

Пример использования библиотеки `leaderelection` для реализации крон-сервиса с выбором лидера. Это гарантирует, что периодические задачи запускаются ровно один раз, даже если множество реплик сервиса работают одновременно.

## Проблема

Когда у вас есть несколько реплик сервиса (Kubernetes, распределённые кластеры и т.д.), стандартный крон запускает одну и ту же задачу на каждой реплике, что приводит к дублированию работы.

Решение: только один "лидер" должен запускать задачи, а остальные реплики остаются в режиме "следователей".

## Основные концепции

### 1. **Выбор лидера через БД**

```go
elector, err := leaderelection.New(leaderelection.Config{
    Store:    pgStore,              // Postgres/MySQL для координации
    Name:     "cron-service",       // Все реплики используют одно имя
    Identity: "replica-1",          // Уникальное имя реплики
    LeaseDuration: 15 * time.Second, // Как долго лидер держит "аренду"
    RenewInterval: 5 * time.Second,  // Как часто обновлять аренду
})
```

- **Store**: Единая БД для всех реплик (Postgres или MySQL)
- **Name**: Одинаковое для всех участников одного выбора
- **Identity**: Уникально для каждой реплики (например, имя пода в Kubernetes)
- **LeaseDuration**: Кто долго лидер может быть в режиме offline перед переизбранием
- **RenewInterval**: Как часто лидер подтверждает своё лидерство

### 2. **Жизненный цикл лидерства**

```go
Callbacks: leaderelection.Callbacks{
    OnStartedLeading: func(ctx context.Context, fence int64) {
        // Текущая реплика стала лидером!
        // Запускаем в отдельной горутине, работаем пока ctx не отменён
        runTasks(ctx)
    },
    OnStoppedLeading: func() {
        // Потеряли лидерство или shutdownимся
        // Очищаем ресурсы, логируем
    },
    OnNewLeader: func(identity string, fence int64) {
        // (Опционально) логирование смены лидера
    },
}
```

**Важно**: 
- `OnStartedLeading` запускается в отдельной горутине
- Когда лидер теряет лидерство, `ctx` отменяется
- Надо срочно остановить работу, когда `ctx.Done()` сработает

### 3. **Два типа задач**

#### A. **Короткие периодические задачи (каждые 15 минут)**

```go
ticker := time.NewTicker(15 * time.Minute)
for {
    select {
    case <-ctx.Done():
        return  // Потеряли лидерство, выходим
    case <-ticker.C:
        // Быстро выполнить задачу, не забывая про ctx
        runQuickTask(ctx)
    }
}
```

**Особенности**:
- Быстро выполняются (секунды, максимум минуты)
- Если потеряли лидерство - сразу останавливаем
- Безопасно запускать в главном цикле

#### B. **Длительные задачи с неизвестной продолжительностью (раз в сутки)**

```go
go runDailyTask(ctx, logger)  // Запускаем в отдельной горутине

func runDailyTask(schedCtx context.Context, logger *slog.Logger) {
    // schedCtx - контекст планировщика
    // Если потеряли лидерство, schedCtx отменится
    
    taskCtx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    taskDone := make(chan error, 1)
    go func() {
        taskDone <- doWork(taskCtx)
    }()
    
    select {
    case err := <-taskDone:
        // Задача завершилась
    case <-schedCtx.Done():
        // Потеряли лидерство
        logger.Info("lost leadership during task, waiting for completion")
        <-taskDone  // Ждём завершения задачи
    }
}
```

**Особенности**:
- Запускается в отдельной горутине
- При потере лидерства не прерывается сразу, дожидается завершения
- Пишет в БД с `fence` токеном, чтобы защитить от грязных записей

## Настройка временных параметров

**Правило**: `RenewInterval + 2*OperationTimeout < LeaseDuration - SafetyMargin`

Стандартные значения (туны для доступа через интернет):
- `LeaseDuration: 15s` - лидер держит "аренду" 15 секунд
- `RenewInterval: 5s` - каждые 5 секунд подтверждает лидерство
- `RetryPeriod: 3s` - если обновление не прошло, повторить через 3s
- `SafetyMargin: 1.5s` - резерв перед истечением аренды

Для локальной БД можно ужать времена:
```go
LeaseDuration:    5 * time.Second,  // Быстрое переизбрание при крахе
RenewInterval:    1 * time.Second,
RetryPeriod:      500 * time.Millisecond,
SafetyMargin:     500 * time.Millisecond,
```

## Защита от грязных записей (Fencing)

Даже если лидер стал "зомби" (процесс завис, но БД обновилась и выбрала нового лидера), старый процесс может отправить устаревшие данные.

Решение: каждая запись включает **fence token** - уникальный номер для каждого периода лидерства.

```go
OnStartedLeading: func(ctx context.Context, fence int64) {
    // fence увеличивается с каждым новым периодом лидерства
    writeToExternalDB(record, fence)  // Пишем fence вместе с данными
}
```

Внешняя система (S3, основная БД, очередь) должна отклонять записи со старыми fence токенами.

## Практический пример

Смотри `example_cron_service.go` для полного рабочего примера:
- `ExampleCronService()` - базовый крон-сервис
- `ExampleFollowerMonitoring()` - как некрон-реплики мониторят лидера
- `ExampleFencing()` - как защитить внешние системы

## Развертывание

### 1. Инициализация схемы БД

Используй SQL из `internal/sqldriver/schema.sql`:
```sql
CREATE TABLE leases (
    id TEXT PRIMARY KEY,
    holder TEXT,
    fence BIGINT,
    expires_at TIMESTAMPTZ,
    ...
);
```

### 2. Конфигурация Store

```go
db, _ := sql.Open("postgres", "postgres://...")
store := sqldriver.New(db)
```

### 3. Запуск в Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cron-service
spec:
  replicas: 3  # 3 реплики, 1 лидер, 2 следователя
  selector:
    matchLabels:
      app: cron-service
  template:
    metadata:
      labels:
        app: cron-service
    spec:
      containers:
      - name: cron-service
        image: my-cron-service:latest
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-creds
              key: url
```

## Гарантии безопасности

1. **At-Most-Once**: Максимум один лидер запускает задачу в один момент времени
2. **Разделение сетей**: Работает даже если реплики в разных кластерах/регионах
3. **Стойкость к сбоям**:
   - Если лидер упал → новый выбирается через `LeaseDuration`
   - Если БД недоступна → процесс сразу перестаёт считать себя лидером
   - Watchdog таймер гарантирует срок отказа от лидерства

## Типичные ошибки

❌ **Неправильно**: Не проверять `ctx.Done()` в длительных задачах
```go
for i := 0; i < 1000000; i++ {
    doWork()  // Может работать час, даже если потеряна реплика!
}
```

✅ **Правильно**:
```go
for i := 0; i < 1000000; i++ {
    select {
    case <-ctx.Done():
        return
    default:
        doWork()
    }
}
```

---

❌ **Неправильно**: Запускать короткие задачи в отдельных горутинах
```go
go runQuickTask()  // Кто будет за ними следить?
```

✅ **Правильно**:
```go
if err := runQuickTask(ctx); err != nil {
    logger.Error("task failed", err)
}
```

---

❌ **Неправильно**: Не использовать fence tokens для внешних записей
```go
writeToS3(data)  // Может быть грязная запись от старого лидера!
```

✅ **Правильно**:
```go
writeToS3(data, fence)  // S3 отклонит запись если fence старый
```
