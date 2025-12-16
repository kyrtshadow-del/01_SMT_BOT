import { useState } from "react";
import { api } from "../../api/client";
import { useStore } from "../../store/useStore";

export const LoginForm = () => {
  const { login } = useStore();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  const handleSubmit: React.FormEventHandler = async (e) => {
    e.preventDefault();
    try {
      const res = await api.post("/api/login", { login: username, password });
      const sid = res.data.session_id as string;
      login(sid);
      setError("");
    } catch {
      setError("Ошибка входа. Проверьте логин/пароль или запустите init_admin.py");
    }
  };

  return (
    <div className="flex h-screen w-full items-center justify-center bg-black text-white">
      <form onSubmit={handleSubmit} className="w-full max-w-sm p-8 space-y-4">
        <h2 className="text-2xl font-bold tracking-tight text-center mb-4">SMT SYSTEM</h2>
        {error && <div className="text-red-500 text-sm text-center mb-2">{error}</div>}
        <input
          className="w-full bg-gray-900 border border-gray-800 p-3 rounded focus:border-emerald-500 outline-none transition-colors"
          placeholder="Login"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
        />
        <input
          className="w-full bg-gray-900 border border-gray-800 p-3 rounded focus:border-emerald-500 outline-none transition-colors"
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <button className="w-full bg-white text-black font-bold p-3 rounded hover:bg-gray-200 transition-colors">
          ENTER
        </button>
      </form>
    </div>
  );
};

