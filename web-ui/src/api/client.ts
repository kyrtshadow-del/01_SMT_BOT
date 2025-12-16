import axios from "axios";
import { useStore } from "../store/useStore";

export const api = axios.create({
  baseURL: "/",
});

api.interceptors.request.use((config) => {
  const token = useStore.getState().sessionId;
  if (token) {
    config.headers = config.headers ?? {};
    config.headers["X-Session-Id"] = token;
  }
  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      useStore.getState().logout();
    }
    return Promise.reject(error);
  }
);

export const fetcher = (url: string) => api.get(url).then((r) => r.data);

