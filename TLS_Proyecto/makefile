# Esto nos ayuda a posicionar nuestros config files en una carpeta dentro de nuestro proyecto
CONFIG_PATH=${HOME}CONFIG_PATH=${HOME}/TLS_Proyecto/


.PHONY: init

init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
# gencert
# First creates the bare certificate, it is just the base certificate that others will differ from
# Then creates the server certificate, this allows our server certification
# Finally we create the client certificate this allows two way authentication
gencert:
	cfssl gencert \
		-initca CA/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=CA/ca-config.json \
		-profile=client \
		-cn="root" \
		CA/client-csr.json | cfssljson -bare client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=CA/ca-config.json \
		-profile=client \
		-cn="nobody" \
		CA/client-csr.json | cfssljson -bare nobody-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=CA/ca-config.json \
		-profile=client \
		-cn="root" \
		CA/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=CA/ca-config.json \
		-profile=client \
		-cn="nobody" \
		CA/client-csr.json | cfssljson -bare nobody-client
	
	# Mover todos los certificados generados a la carpeta de configuración
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.

$(CONFIG_PATH)/model.conf:
	cp ACL/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp ACL/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./...

.PHONY: compile_rpc
compile_rpc:
	protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
