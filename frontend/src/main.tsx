import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { SWRConfig } from 'swr'
import App from './App'
import './styles.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <SWRConfig value={{ revalidateOnFocus: true, dedupingInterval: 30_000, errorRetryCount: 2 }}>
      <App />
    </SWRConfig>
  </StrictMode>,
)
