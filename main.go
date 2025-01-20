package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Employee struct {
	ID         uint   `gorm:"primaryKey"`
	FirstName  string `gorm:"index"`
	LastName   string
	Email      string
	Age        int
	Gender     string
	Department string
	Company    string
	Salary     float64
	DateJoined string
	IsActive   bool
}

var (
	db   *gorm.DB
	logr = logrus.New()
)

func main() {
	initLogger()
	initDB()

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 50<<30) // 50GB limit
		c.Next()
	})

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Welcome to the API",
			"routes": gin.H{
				"/upload":  "POST - Upload a CSV file",
				"/records": "GET - Get paginated records",
				"/count":   "GET - Get total record count",
				"/logs":    "GET - Analyze application logs",
			},
		})
	})

	r.POST("/upload", handleFileUpload)
	r.GET("/records", getPaginatedRecords)
	r.GET("/count", getRowCount)
	r.GET("/logs", analyzeLogs)

	logr.Info("Starting server on port 8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func initLogger() {
	logFile, err := os.OpenFile("logs/app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	logr.Out = logFile
	logr.SetFormatter(&logrus.JSONFormatter{})
	logr.SetLevel(logrus.InfoLevel)
}

func initDB() {
	var err error
	dbcon := "host=postgres user=ArnavJain password=admin dbname=CSV_db port=5432 sslmode=disable TimeZone=UTC"

	for i := 0; i < 10; i++ {
		db, err = gorm.Open(postgres.Open(dbcon), &gorm.Config{})
		if err == nil {
			break
		}
		logr.Warnf("Database not ready, retrying in 5 seconds... (%d/10)", i+1)
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		logr.Fatalf("Failed to connect to database after 10 attempts: %v", err)
	}

	if err := db.AutoMigrate(&Employee{}); err != nil {
		logr.Fatalf("Migration failed: %v", err)
	}
	logr.Info("Database initialized successfully")
}

func handleFileUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		logr.Errorf("Error receiving file: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to upload file"})
		return
	}

	logr.Infof("Received file: %s", file.Filename)

	uploadDir := "./uploads"
	if err := os.MkdirAll(uploadDir, os.ModePerm); err != nil {
		logr.Errorf("Error creating upload directory: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create upload directory"})
		return
	}

	filepath := uploadDir + "/" + file.Filename
	err = c.SaveUploadedFile(file, filepath)
	if err != nil {
		logr.Errorf("Error saving file to %s: %v", filepath, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	logr.Infof("File uploaded successfully to %s", filepath)

	go processCSV(filepath)
	c.JSON(http.StatusOK, gin.H{"message": "File uploaded successfully, processing started"})
}

func processCSV(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		logr.Errorf("Error opening file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		logr.Errorf("Error reading header: %v", err)
		return
	}

	var wg sync.WaitGroup
	ch := make(chan []Employee, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go batchInsert(ch, &wg)
	}

	batch := make([]Employee, 0, 100)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logr.Errorf("Error reading record: %v", err)
			continue
		}

		employee, parseErr := parseRecord(record)
		if parseErr != nil {
			logr.Errorf("Error parsing record: %v", parseErr)
			continue
		}
		batch = append(batch, employee)
		if len(batch) >= 100 {
			ch <- batch
			batch = make([]Employee, 0, 100)
		}
	}

	if len(batch) > 0 {
		ch <- batch
	}

	close(ch)
	wg.Wait()
	logr.Info("CSV processing completed")
}

func parseRecord(record []string) (Employee, error) {
	age, err := strconv.Atoi(record[4])
	if err != nil {
		return Employee{}, err
	}
	salary, err := strconv.ParseFloat(record[8], 64)
	if err != nil {
		return Employee{}, err
	}
	isActive := strings.ToLower(record[10]) == "true"

	return Employee{
		FirstName:  record[1],
		LastName:   record[2],
		Email:      record[3],
		Age:        age,
		Gender:     record[5],
		Department: record[6],
		Company:    record[7],
		Salary:     salary,
		DateJoined: record[9],
		IsActive:   isActive,
	}, nil
}

func batchInsert(ch chan []Employee, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range ch {
		if err := db.Create(&batch).Error; err != nil {
			logr.Errorf("Error inserting batch: %v", err)
		} else {
			logr.Infof("Successfully inserted batch of %d records", len(batch))
		}
	}
}

func getRowCount(c *gin.Context) {
	var count int64
	result := db.Model(&Employee{}).Count(&count)
	if result.Error != nil {
		logr.Errorf("Error counting rows: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count rows"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"total_rows": count})
}

func getPaginatedRecords(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	offset := (page - 1) * limit

	sort := c.DefaultQuery("sort", "id")
	order := c.DefaultQuery("order", "asc")

	var employees []Employee
	result := db.Order(sort + " " + order).Limit(limit).Offset(offset).Find(&employees)
	if result.Error != nil {
		logr.Errorf("Error retrieving paginated records: %v", result.Error)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve records"})
		return
	}

	c.JSON(http.StatusOK, employees)
}

func analyzeLogs(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	level := c.Query("level")
	source := c.Query("source")

	logFile := "logs/app.log"
	content, err := os.ReadFile(logFile)
	if err != nil {
		logr.Errorf("Error reading log file: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read log file"})
		return
	}

	var filteredLogs []map[string]interface{}
	logs := strings.Split(string(content), "\n")
	for _, logLine := range logs {
		if logLine == "" {
			continue
		}

		var logEntry map[string]interface{}
		if err := json.Unmarshal([]byte(logLine), &logEntry); err != nil {
			logr.Errorf("Error parsing log entry: %v", err)
			continue
		}

		if level != "" && logEntry["level"] != level {
			continue
		}

		if startDate != "" || endDate != "" {
			logTime, err := time.Parse(time.RFC3339, logEntry["time"].(string))
			if err != nil {
				logr.Errorf("Error parsing log time: %v", err)
				continue
			}
			if startDate != "" {
				start, _ := time.Parse("2006-01-02", startDate)
				if logTime.Before(start) {
					continue
				}
			}
			if endDate != "" {
				end, _ := time.Parse("2006-01-02", endDate)
				if logTime.After(end) {
					continue
				}
			}
		}

		if source != "" && logEntry["source"] != source {
			continue
		}

		filteredLogs = append(filteredLogs, logEntry)
	}

	c.JSON(http.StatusOK, gin.H{"logs": filteredLogs})
}
