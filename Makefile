PROTO_DIR := src/main/proto
PROTO_SRC := $(PROTO_DIR)/api/v1
JAVA_OUT_DIR := target/generated-sources/protobuf/java

PROTOC := protoc

PROTO_FILES := $(wildcard $(PROTO_SRC)/*.proto)

all: compile

compile: $(PROTO_FILES)
	@echo "Generating Java classes from .proto files..."
	@mkdir -p $(JAVA_OUT_DIR)
	$(PROTOC) --java_out=$(JAVA_OUT_DIR) -I=$(PROTO_DIR) $(PROTO_FILES)
	@echo "Done!"

clean:
	@echo "Cleaning generated protobuf Java files..."
	rm -rf $(JAVA_OUT_DIR)
	@echo "Done!"
