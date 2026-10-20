import React, { useState } from 'react';
import axios from 'axios';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid } from 'recharts';
import './App.css';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8086';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];

function App() {
  const [userId, setUserId] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  const fetchData = async () => {
    if (!userId) {
      setError('Please enter a user ID');
      return;
    }

    setLoading(true);
    setError(null);
    
    try {
      const response = await axios.get(`${API_URL}/api/user/${userId}/routing`);
      setData(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to fetch data. Make sure the Dashboard API is running.');
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  // Prepare pie chart data
  const pieData = data ? [
    { name: 'Inference-1', value: data.routing_breakdown['inference-1'] || 0 },
    { name: 'Inference-2', value: data.routing_breakdown['inference-2'] || 0 }
  ] : [];

  // Prepare bar chart data for inference-1 responses
  const inference1Data = data?.responses?.['inference-1']?.map((item, index) => ({
    index: index + 1,
    response: item.response_text,
    timestamp: item.timestamp ? new Date(item.timestamp).toLocaleString() : ''
  })) || [];

  // Prepare bar chart data for inference-2 responses
  const inference2Data = data?.responses?.['inference-2']?.map((item, index) => ({
    index: index + 1,
    response: item.response_text,
    timestamp: item.timestamp ? new Date(item.timestamp).toLocaleString() : ''
  })) || [];

  return (
    <div className="App">
      <header className="App-header">
        <h1>Movie Recommendation Routing Dashboard</h1>
      </header>
      
      <div className="container">
        <div className="input-section">
          <div className="input-group">
            <label htmlFor="userId">User ID:</label>
            <input
              id="userId"
              type="number"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
              placeholder="Enter user ID"
              onKeyPress={(e) => e.key === 'Enter' && fetchData()}
            />
            <button onClick={fetchData} disabled={loading}>
              {loading ? 'Loading...' : 'Show Routing History'}
            </button>
          </div>
          {error && <div className="error">{error}</div>}
        </div>

        {data && (
          <div className="dashboard">
            {/* Pie Chart - Routing Breakdown */}
            <div className="chart-container">
              <h2>Routing Distribution</h2>
              <p>User {data.user_id} - Last {data.hours} hours</p>
              {pieData.some(item => item.value > 0) ? (
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={pieData}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {pieData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <p className="no-data">No routing data available for this user.</p>
              )}
            </div>

            {/* Bar Chart - Inference-1 Responses */}
            <div className="chart-container">
              <h2>Inference-1 Responses</h2>
              <p>Total: {inference1Data.length} responses</p>
              {inference1Data.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={inference1Data}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="index" 
                      label={{ value: 'Request #', position: 'insideBottom', offset: -5 }}
                    />
                    <YAxis 
                      label={{ value: 'Movie IDs', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip 
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          return (
                            <div className="custom-tooltip">
                              <p>{`Response: ${payload[0].payload.response}`}</p>
                              <p>{`Time: ${payload[0].payload.timestamp}`}</p>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    <Bar dataKey="response" fill="#0088FE" />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <p className="no-data">No responses from inference-1 for this user.</p>
              )}
            </div>

            {/* Bar Chart - Inference-2 Responses */}
            <div className="chart-container">
              <h2>Inference-2 Responses</h2>
              <p>Total: {inference2Data.length} responses</p>
              {inference2Data.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={inference2Data}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis 
                      dataKey="index" 
                      label={{ value: 'Request #', position: 'insideBottom', offset: -5 }}
                    />
                    <YAxis 
                      label={{ value: 'Movie IDs', angle: -90, position: 'insideLeft' }}
                    />
                    <Tooltip 
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          return (
                            <div className="custom-tooltip">
                              <p>{`Response: ${payload[0].payload.response}`}</p>
                              <p>{`Time: ${payload[0].payload.timestamp}`}</p>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    <Bar dataKey="response" fill="#00C49F" />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <p className="no-data">No responses from inference-2 for this user.</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
