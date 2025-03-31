<template>
  <v-container>
    <v-row>
      <v-col cols="12">
        <h1 class="text-h4 mb-4">Incrementality Tool</h1>
        <v-card>
          <v-card-title>
            <v-row>
              <v-col cols="5">
                <v-text-field
                  v-model="brandId"
                  label="Brand ID"
                  outlined
                  clearable
                  type="number"
                ></v-text-field>
              </v-col>
              <v-col cols="5">
                <v-text-field
                  v-model="brandName"
                  label="Brand Name"
                  outlined
                  clearable
                ></v-text-field>
              </v-col>
              <v-col cols="2" class="d-flex align-center">
                <v-btn
                  color="primary"
                  @click="handleQuery"
                  :disabled="!brandId && !brandName"
                >
                  Query Metrics
                </v-btn>
              </v-col>
            </v-row>
          </v-card-title>
          <v-card-text>
            <v-data-table
              :headers="headers"
              :items="brands"
              :loading="loading"
              hide-default-footer
            ></v-data-table>
            <v-divider class="my-4"></v-divider>
            <v-card v-if="metrics" class="mt-4">
              <v-card-title>Metrics</v-card-title>
              <v-card-text>
                <v-row>
                  <v-col cols="3">
                    <v-text-field
                      label="iRoas"
                      v-model="metrics.iRoas"
                      readonly
                      outlined
                    ></v-text-field>
                  </v-col>
                  <v-col cols="3">
                    <v-text-field
                      label="Roas"
                      v-model="metrics.Roas"
                      readonly
                      outlined
                    ></v-text-field>
                  </v-col>
                  <v-col cols="3">
                    <v-text-field
                      label="Incremental Factor"
                      v-model="metrics.incremental_factor"
                      readonly
                      outlined
                    ></v-text-field>
                  </v-col>
                  <v-col cols="3">
                    <v-text-field
                      label="Spend"
                      v-model="metrics.spend"
                      readonly
                      outlined
                    ></v-text-field>
                  </v-col>
                </v-row>
                <v-row>
                  <v-col cols="12">
                    <canvas ref="chart" style="width: 100%; height: 400px; min-width: 600px; min-height: 400px;"></canvas>
                  </v-col>
                </v-row>
              </v-card-text>
            </v-card>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup>
import { ref, onMounted, nextTick } from 'vue'
import { Chart } from 'chart.js/auto'

const brandId = ref('')
const brandName = ref('')
const brands = ref([])
const metrics = ref(null)
const loading = ref(false)
const dialog = ref(false)
const headers = [
  { text: 'Brand ID', value: 'brand_id' },
  { text: 'Brand Name', value: 'brand_name' }
]

async function handleQuery() {
  if (brandId.value) {
    getMetrics()
  } else if (brandName.value) {
    // 根据名称查询品牌信息
    try {
      const response = await fetch(`/api/v1/brands?search=${brandName.value}`)
      const data = await response.json()
      if (data.data.length > 0) {
        brandId.value = data.data[0].brand_id
        getMetrics()
      }
    } catch (error) {
      console.error('Error fetching brand by name:', error)
    }
  }
}

function clearData() {
  metrics.value = null
  if (chartInstance) {
    chartInstance.destroy()
    chartInstance = null
  }
}

const chart = ref(null)
let chartInstance = null

function renderChart(weeks, iRoasData, RoasData) {
  // 等待DOM更新
  nextTick(() => {
    try {
      if (!chart.value) {
        console.error('Chart container not found')
        return
      }
    
      // 验证数据
      if (!Array.isArray(weeks) || !Array.isArray(iRoasData) || !Array.isArray(RoasData)) {
        console.error('Invalid data format for chart')
        return
      }
    
      // 如果已有图表实例，先销毁
      if (chartInstance) {
        chartInstance.destroy()
      }
      
      const ctx = chart.value.getContext('2d')
      if (!ctx) {
        console.error('Could not get canvas context')
        return
      }
      
        chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
          labels: weeks,
          datasets: [
            {
              label: 'iRoas',
              data: iRoasData,
              borderColor: '#3e95cd',
              backgroundColor: '#3e95cd',
              fill: false,
              tension: 0.4,
              borderWidth: 2
            },
            {
              label: 'Roas',
              data: RoasData,
              borderColor: '#8e5ea2',
              backgroundColor: '#8e5ea2',
              fill: false,
              tension: 0.4,
              borderWidth: 2
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            mode: 'index',
            intersect: false
          },
          scales: {
            x: {
              display: true,
              title: {
                display: true,
                text: 'Week'
              },
              grid: {
                display: false
              }
            },
            y: {
              display: true,
              title: {
                display: true,
                text: 'Value'
              },
              beginAtZero: true,
              grid: {
                color: '#e0e0e0'
              }
            }
          },
          plugins: {
            legend: {
              position: 'top',
            },
            tooltip: {
              enabled: true
            }
          }
        }
      })
    } catch (error) {
      console.error('Error rendering chart:', error)
    }
  })
}

async function getMetrics() {
  console.log('getMetrics called with brandId:', brandId.value)
  if (!brandId.value) {
    alert('Please enter a valid Brand ID')
    return
  }
  loading.value = true
  console.log('Fetching metrics for brandId:', brandId.value)
  
  try {
    // 获取周度数据
    const weeklyResponse = await fetch(`/api/v1/metrics/${brandId.value}/weekly`)
    if (!weeklyResponse.ok) {
      throw new Error(`HTTP error! status: ${weeklyResponse.status}`)
    }
    const weeklyData = await weeklyResponse.json()
    console.log('Weekly Metrics API Response:', weeklyData)
    
    if (weeklyData && weeklyData.data && weeklyData.data.length > 0) {
      // 计算平均指标
      const totalWeeks = weeklyData.data.length
      const totalIroas = weeklyData.data.reduce((sum, item) => sum + (item.totalIroas || 0), 0)
      const totalSpIroas = weeklyData.data.reduce((sum, item) => sum + (item.spIroas || 0), 0)
      
      metrics.value = {
        iRoas: (totalIroas / totalWeeks).toFixed(2),
        Roas: (totalSpIroas / totalWeeks).toFixed(2),
        incremental_factor: 'N/A',
        spend: 'N/A'
      }
      
      // 处理周度数据
        console.log('Weekly Data:', weeklyData)
        console.log('Raw weekly data:', weeklyData)
        const weeks = weeklyData.data.map(item => {
          const date = new Date(item.week_start_date)
          if (isNaN(date)) {
            console.error('Invalid date:', item.week_start_date)
            return 'Invalid Date'
          }
          return date.toLocaleDateString()
        })
        const iRoasData = weeklyData.data.map(item => {
          const value = parseFloat(item.totalIroas)
          if (isNaN(value)) {
            console.error('Invalid totalIroas value:', item.totalIroas)
            return 0
          }
          return value
        })
        const RoasData = weeklyData.data.map(item => {
          const value = parseFloat(item.spIroas)
          if (isNaN(value)) {
            console.error('Invalid spIroas value:', item.spIroas)
            return 0
          }
          return value
        })
        console.log('Processed data:')
        console.log('Weeks:', weeks)
        console.log('iRoasData:', iRoasData)
        console.log('RoasData:', RoasData)
        console.log('Data length check:')
        console.log('Weeks length:', weeks.length)
        console.log('iRoasData length:', iRoasData.length)
        console.log('RoasData length:', RoasData.length)
        renderChart(weeks, iRoasData, RoasData)
      } else {
        renderChart([], [], [])
      }
    } 
   catch (error) {
    console.error('Error fetching metrics:', error)
    metrics.value = null
    alert(`Error fetching metrics: ${error.message}`)
  } finally {
    loading.value = false
  }
}

</script>
