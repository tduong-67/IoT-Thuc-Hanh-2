#include <Arduino.h>

// Bài thực hành 2 - IoT: ESP32 + DHT22 + HC-SR04 + LED -> MQTT (TLS) -> HiveMQ Cloud
// Chạy trên Wokwi (VS Code). Điền 3 dòng CẤU HÌNH MQTT bên dưới trước khi build.

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <sys/time.h>


const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";

const char* MQTT_HOST = "7a9e69279ef9407abe4cfc49fa07539e.s1.eu.hivemq.cloud";
const int   MQTT_PORT = 8883;
const char* MQTT_USER = "Duong067";
const char* MQTT_PASS = "dtduong067@";

const char* DEVICE_ID = "esp32-01";
const char* TOPIC     = "iot/lab2/esp32-01/telemetry";

const int DHT_PIN  = 15;  
const int TRIG_PIN = 5;  
const int ECHO_PIN = 18;  
const int LED_PIN  = 4; 

const float LED_ON_CM  = 30.0;  
const float LED_OFF_CM = 40.0;  

// ===== THAM SỐ =====
const unsigned long PUBLISH_MS = 3000;
const bool INJECT_FAULTS = true;  

DHT dht(DHT_PIN, DHT22);
WiFiClientSecure net;
PubSubClient mqtt(net);

unsigned long seq = 0;
unsigned long lastPublish = 0;
bool ledOn = false;

uint64_t nowMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

void connectWifi() {
  WiFi.begin(WIFI_SSID, WIFI_PASS, 6);
  while (WiFi.status() != WL_CONNECTED) {
    delay(250);
    Serial.print(".");
  }
  Serial.println("\nWiFi OK");
}

void syncTime() {
  configTime(0, 0, "pool.ntp.org", "time.google.com");
  while (time(nullptr) < 1700000000) {
    delay(200);
  }
  Serial.println("NTP OK");
}

void connectMqtt() {
  while (!mqtt.connected()) {
    String clientId = String(DEVICE_ID) + "-" + String((uint32_t)random(0xffff), HEX);
    if (mqtt.connect(clientId.c_str(), MQTT_USER, MQTT_PASS)) {
      Serial.println("MQTT OK");
    } else {
      Serial.printf("MQTT fail rc=%d, thử lại sau 3s\n", mqtt.state());
      delay(3000);
    }
  }
}


float readDistanceCm() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);

  unsigned long duration = pulseIn(ECHO_PIN, HIGH, 30000UL); 
  if (duration == 0) return NAN;

  float cm = duration * 0.0343f / 2.0f; 
  if (cm < 2.0f || cm > 400.0f) return NAN;
  return cm;
}


void updateLed(float distanceCm) {
  if (!ledOn && distanceCm < LED_ON_CM) {
    ledOn = true;
  } else if (ledOn && distanceCm > LED_OFF_CM) {
    ledOn = false;
  }
  digitalWrite(LED_PIN, ledOn ? HIGH : LOW);
}

void publishReading() {
  float t = dht.readTemperature();
  float h = dht.readHumidity();
  float d = readDistanceCm();

  if (isnan(d)) {
    Serial.println("HC-SR04 không có phản hồi hợp lệ");
    return;
  }
  updateLed(d); 

  if (isnan(t) || isnan(h)) {
    Serial.println("DHT read fail");
    return;
  }

  seq++;
  bool duplicate = false;

  if (INJECT_FAULTS) {
    int r = random(100);
    if (r < 3) {                       // mất gói: seq bị nhảy
      Serial.printf("[FAULT] drop seq=%lu\n", seq);
      return;
    } else if (r < 6) {                // outlier: vẫn nằm trong khoảng hợp lệ
      t += 20.0f;
      d = min(d + 100.0f, 400.0f);
      Serial.printf("[FAULT] spike seq=%lu\n", seq);
    } else if (r < 8) {                // gửi trùng
      duplicate = true;
    }
  }

  char payload[224];
  snprintf(payload, sizeof(payload),
           "{\"device_id\":\"%s\",\"seq\":%lu,\"ts_publish\":%llu,"
           "\"temperature\":%.1f,\"humidity\":%.1f,\"distance_cm\":%.1f,\"led\":%d}",
           DEVICE_ID, seq, (unsigned long long)nowMs(), t, h, d, ledOn ? 1 : 0);

  mqtt.publish(TOPIC, payload);
  if (duplicate) {
    mqtt.publish(TOPIC, payload);
    Serial.println("[FAULT] duplicate");
  }
  Serial.println(payload);
}

void setup() {
  Serial.begin(115200);
  dht.begin();
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  randomSeed(esp_random());

  connectWifi();
  syncTime();

  net.setInsecure();
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectWifi();
  if (!mqtt.connected()) connectMqtt();
  mqtt.loop();

  if (millis() - lastPublish >= PUBLISH_MS) {
    lastPublish = millis();
    publishReading();
  }
}