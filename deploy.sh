#!/bin/bash

# 安装依赖
echo "安装系统依赖..."
sudo apt-get update
sudo apt-get install -y nginx python3 python3-pip nodejs npm

# 安装Python依赖
echo "安装Python依赖..."
pip3 install -r requirements.txt

# 安装Node.js依赖
echo "安装Node.js依赖..."
npm install

# 构建前端
echo "构建前端..."
npm run build

# 配置Nginx
echo "配置Nginx..."
sudo tee /etc/nginx/sites-available/incrementality_tool <<EOF
server {
    listen 80;
    server_name your_domain_or_ip;

    location / {
        root /path/to/your/project/dist;
        try_files \$uri /index.html;
    }

    location /api/ {
        proxy_pass http://localhost:3000/;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    }
}
EOF

# 启用Nginx配置
sudo ln -sf /etc/nginx/sites-available/incrementality_tool /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl restart nginx

# 设置服务自启动
echo "设置服务自启动..."
sudo tee /etc/systemd/system/incrementality_tool.service <<EOF
[Unit]
Description=Incrementality Tool Service
After=network.target

[Service]
User=your_user
WorkingDirectory=/path/to/your/project
ExecStart=/usr/bin/python3 src/backend/app.py
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable incrementality_tool
sudo systemctl start incrementality_tool

echo "部署完成！"
