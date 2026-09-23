# %% [markdown]
# # C2PhyNet — Physics-Enhanced Network for Predicting Sequential Typhoon Cloud Images
# Reproduction of Yuan et al., *IEEE JSTARS 2025* (C2PhyNet), theo đúng đặc tả trong
# `C2PhyNet_compact_prompt.md`, gộp thành **một notebook duy nhất**.
# 
# **Thứ tự notebook:**
# 1. Setup & cấu hình
# 2. Các module con (SCSE, Encoder/Decoder, ConvLSTM, C2-Enhancer, Advection, Input Assimilation, Phy-Predictor, Phy-Unit)
# 3. Full model `C2PhyNet`
# 4. Dataset (sliding-window, lazy-load)
# 5. Physics/Moment loss
# 6. Unit test / kiểm tra shape (chạy ngay với dữ liệu random — không cần dataset thật)
# 7. Training loop (cần chỉnh `DataConfig.root_dir` trỏ tới ảnh Himawari-8/9 B13 thật)
# 8. Evaluation (MSE/MAE/SSIM/PSNR)
# 9. Autoregressive inference
# 10. Sơ đồ kiến trúc ASCII
# 11. Bảng đối chiếu công thức bài báo ↔ code
# 
# > Chạy tuần tự từ trên xuống. Phần 6 (unit test) chạy được ngay không cần GPU/dataset. Phần 7-9 cần dữ liệu thật và (khuyến nghị) GPU RTX 4070 Ti Super như trong bài báo/spec gốc.
# 

# %% [markdown]
# ## 1. Setup

# %%
# [ĐÃ TẮT] import os
# [ĐÃ TẮT] print(os.getcwd())
# [ĐÃ TẮT] print(os.path.exists("encoder.pth"), os.path.exists("decoder.pth"))

# %%
# Cài đặt (bỏ qua nếu môi trường đã có sẵn)
# !pip install torch pillow -q

import os, math, argparse
from dataclasses import dataclass, field
from typing import List, Sequence

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader

print("Torch:", torch.__version__, "| CUDA available:", torch.cuda.is_available())
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# %% [markdown]
# ### Config (`config.py` gốc)

# %%
from dataclasses import dataclass, field


@dataclass
class ModelConfig:
    in_channels: int = 1
    out_channels: int = 1
    hidden_channels: int = 64
    base_channels: int = 32
    num_stages: int = 3  # 3 downsample stages -> factor 8 (512 -> 64)


@dataclass
class DataConfig:
    root_dir: str = "data/himawari_b13"
    image_size: int = 512
    n_in: int = 8
    n_out: int = 1
    stride: int = 1
    num_workers: int = 2


@dataclass
class TrainConfig:
    batch_size: int = 2
    epochs: int = 100
    lr: float = 1e-4
    weight_decay: float = 1e-5
    lambda_moment: float = 1.0
    use_ssim: bool = False
    lambda_ssim: float = 0.0
    grad_accum_steps: int = 1
    grad_clip_norm: float = 1.0
    amp: bool = True
    checkpoint_dir: str = "checkpoints"
    log_every: int = 20
    val_every: int = 1


@dataclass
class Config:
    model: ModelConfig = field(default_factory=ModelConfig)
    data: DataConfig = field(default_factory=DataConfig)
    train: TrainConfig = field(default_factory=TrainConfig)
    device: str = "cuda"
    seed: int = 42

# %% [markdown]
# ## 2. Các module con

# %% [markdown]
# ### 2.1 SCSE-Attention — Eq.(11)-(16)

# %%
"""
SCSE-Attention (Concurrent Spatial and Channel Squeeze-and-Excitation)
Paper: Sec. III-B, Eqs. (11)-(16). NOT SE, NOT CBAM.

C-SE (channel squeeze, channel excite):
    z_k = (1/HW) sum_ij U_k(i,j)              (11)
    z_hat = W1( ReLU( W2 z ) )                (12)
    U_C-SE = sigmoid(z_hat) * U               (13)

S-SE (channel squeeze, spatial excite):
    q = Wsq * U   (1x1 conv, C->1)            (14)
    U_S-SE = sigmoid(q) * U                   (15)

U_SCSE = U_C-SE + U_S-SE                       (16)
"""
import torch
import torch.nn as nn


class SCSEAttention(nn.Module):
    def __init__(self, channels: int, reduction: int = 8):
        super().__init__()
        c2 = max(channels // reduction, 1)
        # C-SE: global-avg-pool -> W2 (C->C2) -> ReLU -> W1 (C2->C) -> sigmoid
        self.avg_pool = nn.AdaptiveAvgPool2d(1)  # Eq (11)
        self.w2 = nn.Linear(channels, c2, bias=True)
        self.relu = nn.ReLU(inplace=True)
        self.w1 = nn.Linear(c2, channels, bias=True)  # Eq (12)
        self.sigmoid = nn.Sigmoid()

        # S-SE: 1x1 conv C->1 (Wsq), sigmoid
        self.w_sq = nn.Conv2d(channels, 1, kernel_size=1, bias=True)  # Eq (14)

    def forward(self, u: torch.Tensor) -> torch.Tensor:
        b, c, h, w = u.shape

        # ---- C-SE ----
        z = self.avg_pool(u).view(b, c)                 # Eq (11): z_k
        z_hat = self.w1(self.relu(self.w2(z)))           # Eq (12)
        u_cse = self.sigmoid(z_hat).view(b, c, 1, 1) * u  # Eq (13)

        # ---- S-SE ----
        q = self.w_sq(u)                                  # Eq (14): q in R^{H x W}
        u_sse = self.sigmoid(q) * u                        # Eq (15)

        return u_cse + u_sse  # Eq (16)

# %% [markdown]
# ### 2.2 Encoder — E(u)

# %%
"""
Encoder block: (Conv -> Norm -> Act -> Downsample) x N, each stage + SCSE-Attention.
Paper does not give exact conv configs (channel counts, depth) -> [AMBIGUOUS IN PAPER].
We expose it as configurable; defaults chosen for 512x512 B13 grayscale input.
"""
import torch
import torch.nn as nn


class ConvBlock(nn.Module):
    def __init__(self, in_ch, out_ch, downsample=True, norm="batch", act="relu"):
        super().__init__()
        stride = 2 if downsample else 1
        self.conv = nn.Conv2d(in_ch, out_ch, kernel_size=3, stride=stride, padding=1)
        self.norm = nn.BatchNorm2d(out_ch) if norm == "batch" else nn.GroupNorm(8, out_ch)
        self.act = nn.ReLU(inplace=True) if act == "relu" else nn.LeakyReLU(0.2, inplace=True)
        self.scse = SCSEAttention(out_ch)

    def forward(self, x):
        x = self.act(self.norm(self.conv(x)))
        x = self.scse(x)
        return x


class Encoder(nn.Module):
    """
    E(u): u [B,C,H,W] -> encoded feature h in potential space H, [B,C_hidden,H/8,W/8].
    [AMBIGUOUS IN PAPER]: exact depth/channels not specified. Default: 3 downsample
    stages, 1 -> 32 -> 64 -> hidden_channels.
    """
    def __init__(self, in_channels=1, hidden_channels=64, base_channels=32, num_stages=3):
        super().__init__()
        chans = [in_channels] + [base_channels * (2 ** i) for i in range(num_stages - 1)] + [hidden_channels]
        blocks = []
        for i in range(num_stages):
            blocks.append(ConvBlock(chans[i], chans[i + 1], downsample=True))
        self.blocks = nn.ModuleList(blocks)
        self.out_channels = hidden_channels

    def forward(self, x):
        for b in self.blocks:
            x = b(x)
        return x

# %% [markdown]
# ### 2.3 Decoder — D(h)

# %%
"""
Decoder block: mirrors Encoder. (Deconv/Upsample -> Conv -> Norm -> Act) x N,
each stage + SCSE-Attention. Final 1x1 conv -> [B,1,H,W].
[AMBIGUOUS IN PAPER]: exact depth/channels not specified; mirrors Encoder defaults.
"""
import torch
import torch.nn as nn


class DeconvBlock(nn.Module):
    def __init__(self, in_ch, out_ch, norm="batch", act="relu"):
        super().__init__()
        self.deconv = nn.ConvTranspose2d(in_ch, out_ch, kernel_size=4, stride=2, padding=1)
        self.norm = nn.BatchNorm2d(out_ch) if norm == "batch" else nn.GroupNorm(8, out_ch)
        self.act = nn.ReLU(inplace=True) if act == "relu" else nn.LeakyReLU(0.2, inplace=True)
        self.scse = SCSEAttention(out_ch)

    def forward(self, x):
        x = self.act(self.norm(self.deconv(x)))
        x = self.scse(x)
        return x


class Decoder(nn.Module):
    """D: h [B,C_hidden,H/8,W/8] -> u_pred [B,out_channels,H,W]."""
    def __init__(self, hidden_channels=64, base_channels=32, out_channels=1, num_stages=3):
        super().__init__()
        chans = [hidden_channels] + [base_channels * (2 ** i) for i in reversed(range(num_stages - 1))]
        blocks = []
        for i in range(num_stages - 1):
            blocks.append(DeconvBlock(chans[i], chans[i + 1]))
        # last stage: deconv straight to out_channels, then sigmoid (images in [0,1])
        self.blocks = nn.ModuleList(blocks)
        self.final_deconv = nn.ConvTranspose2d(chans[-1], out_channels, kernel_size=4, stride=2, padding=1)
        self.act_out = nn.Sigmoid()

    def forward(self, x):
        for b in self.blocks:
            x = b(x)
        x = self.final_deconv(x)
        return self.act_out(x)

# %%
import torch
import torch.nn as nn
import torch.nn.functional as F


class PretrainedEncoder(nn.Module):
    """Kiến trúc khớp với encoder của CAE đã train (conv1..conv4),
    có thêm SCSE-Attention sau mỗi stage để tương thích với phần còn lại của C2PhyNet."""

    def __init__(self, in_channels: int = 1):
        super().__init__()
        self.conv1 = nn.Conv2d(in_channels, 32, kernel_size=3, padding=1)
        self.pool1 = nn.MaxPool2d(kernel_size=2)
        self.scse1 = SCSEAttention(32)

        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.pool2 = nn.MaxPool2d(kernel_size=2)
        self.scse2 = SCSEAttention(64)

        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, padding=1)
        self.bn3 = nn.BatchNorm2d(128)
        self.pool3 = nn.MaxPool2d(kernel_size=2)
        self.scse3 = SCSEAttention(128)

        self.conv4 = nn.Conv2d(128, 256, kernel_size=3, padding=1)
        self.bn4 = nn.BatchNorm2d(256)
        self.scse4 = SCSEAttention(256)

        self.out_channels = 256

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.scse1(self.pool1(F.relu(self.conv1(x))))
        x = self.scse2(self.pool2(F.relu(self.conv2(x))))
        x = self.scse3(self.pool3(F.relu(self.bn3(self.conv3(x)))))
        x = self.scse4(F.relu(self.bn4(self.conv4(x))))
        return x  # [B, 256, H/8, W/8]


class PretrainedDecoder(nn.Module):
    """Khớp với decoder của CAE đã train (deconv1..deconv4 + conv_out),
    có thêm SCSE-Attention sau mỗi stage."""

    def __init__(self, out_channels: int = 1):
        super().__init__()
        self.deconv1 = nn.ConvTranspose2d(256, 256, kernel_size=3, stride=1, padding=1)
        self.scse1 = SCSEAttention(256)

        self.deconv2 = nn.ConvTranspose2d(256, 128, kernel_size=4, stride=2, padding=1)
        self.scse2 = SCSEAttention(128)

        self.deconv3 = nn.ConvTranspose2d(128, 64, kernel_size=4, stride=2, padding=1)
        self.scse3 = SCSEAttention(64)

        self.deconv4 = nn.ConvTranspose2d(64, 32, kernel_size=4, stride=2, padding=1)
        self.scse4 = SCSEAttention(32)

        self.conv_out = nn.Conv2d(32, out_channels, kernel_size=3, padding=1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.scse1(F.relu(self.deconv1(x)))
        x = self.scse2(F.relu(self.deconv2(x)))
        x = self.scse3(F.relu(self.deconv3(x)))
        x = self.scse4(F.relu(self.deconv4(x)))
        return torch.sigmoid(self.conv_out(x))

# %% [markdown]
# ### 2.4 ConvLSTM — nhánh h^r

# %%
"""
True ConvLSTM (Shi et al. 2015, cited in paper as [9]).
hr(t+1) = ConvLSTM(hr(t), E(u(t)))
Standard convolutional gates: i, f, o, g (candidate), hidden/cell state [B,C,H,W].
"""
import torch
import torch.nn as nn


class ConvLSTMCell(nn.Module):
    def __init__(self, input_channels, hidden_channels, kernel_size=3):
        super().__init__()
        pad = kernel_size // 2
        # single conv producing all 4 gates at once, taking [x_t, h_{t-1}] concatenated
        self.conv = nn.Conv2d(
            input_channels + hidden_channels,
            4 * hidden_channels,
            kernel_size=kernel_size,
            padding=pad,
        )
        self.hidden_channels = hidden_channels

    def forward(self, x, state):
        h_prev, c_prev = state
        combined = torch.cat([x, h_prev], dim=1)
        gates = self.conv(combined)
        i, f, o, g = torch.chunk(gates, 4, dim=1)
        i = torch.sigmoid(i)
        f = torch.sigmoid(f)
        o = torch.sigmoid(o)
        g = torch.tanh(g)
        c = f * c_prev + i * g
        h = o * torch.tanh(c)
        return h, c

    def init_state(self, batch_size, height, width, device):
        h0 = torch.zeros(batch_size, self.hidden_channels, height, width, device=device)
        c0 = torch.zeros(batch_size, self.hidden_channels, height, width, device=device)
        return h0, c0

# %% [markdown]
# ### 2.5 Learnable derivative kernels (Sobel-init) — Eq.(6)-(8)

# %%
"""
Learnable derivative (Sobel-initialized) kernels used to approximate d/dx, d/dy
on the kinematic field V, per Eq. (6)-(8).

kx = [[-1,0,1],[-2,0,2],[-1,0,1]]   (Sobel x)
ky = [[-1,-2,-1],[0,0,0],[1,2,1]]   (Sobel y)

wx, wy are LEARNABLE (initialized from kx, ky, not fixed) as the paper states it
allowed the kernels to be learnable to adaptively approximate Sobel during
training. L_moment = ||wx - kx||_F + ||wy - ky||_F (Eq. 8) pulls them back
toward the analytic Sobel kernels.
"""
import torch
import torch.nn as nn
import torch.nn.functional as F

_KX = torch.tensor([[-1., 0., 1.],
                     [-2., 0., 2.],
                     [-1., 0., 1.]])
_KY = torch.tensor([[-1., -2., -1.],
                     [0., 0., 0.],
                     [1., 2., 1.]])


class LearnableDerivative(nn.Module):
    """Single-channel depthwise 3x3 conv, weight initialized from a Sobel kernel."""

    def __init__(self, init_kernel: torch.Tensor):
        super().__init__()
        self.weight = nn.Parameter(init_kernel.clone().view(1, 1, 3, 3))
        self.register_buffer("target_kernel", init_kernel.clone().view(1, 1, 3, 3))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [B,1,H,W] -> depthwise conv, same padding
        return F.conv2d(x, self.weight, padding=1)

    def moment_loss(self) -> torch.Tensor:
        # Eq. (8) term: Frobenius norm of (w - k)
        return torch.norm(self.weight - self.target_kernel, p="fro")


class DerivativeOperator(nn.Module):
    """Holds both wx and wy and reports the combined moment loss (Eq. 8)."""

    def __init__(self):
        super().__init__()
        self.dx = LearnableDerivative(_KX)
        self.dy = LearnableDerivative(_KY)

    def divergence(self, vx: torch.Tensor, vy: torch.Tensor):
        """div V = dVx/dx + dVy/dy, each of vx, vy: [B,1,H,W]."""
        dvx_dx = self.dx(vx)
        dvy_dy = self.dy(vy)
        div_v = dvx_dx + dvy_dy
        return div_v, dvx_dx, dvy_dy

    def moment_loss(self) -> torch.Tensor:
        return self.dx.moment_loss() + self.dy.moment_loss()

# %% [markdown]
# ### 2.6 AdvectionItems — Eq.(3), Eq.(6)

# %%
class AdvectionItems(nn.Module):
    def __init__(self, channels: int, dt_init: float = 0.1, div_clamp: float = 5.0):
        super().__init__()
        self.to_v = nn.Conv2d(channels, 2, kernel_size=1)
        self.deriv = DerivativeOperator()
        # dt học được nhưng luôn dương và có trần, đóng vai trò như bước thời gian CFL
        self.log_dt = nn.Parameter(torch.log(torch.tensor(dt_init)))
        self.div_clamp = div_clamp

    def forward(self, hp: torch.Tensor):
        v = self.to_v(hp)
        vx, vy = v[:, 0:1], v[:, 1:2]
        div_v, dvx_dx, dvy_dy = self.deriv.divergence(vx, vy)

        # điều kiện ổn định kiểu CFL: chặn div_v trước khi dùng
        div_v = torch.clamp(div_v, -self.div_clamp, self.div_clamp)
        dt = torch.exp(self.log_dt).clamp(max=1.0)

        # broadcast nhân trực tiếp — tương đương matmul cũ nhưng rẻ hơn nhiều
        # và không tạo tensor trung gian (B*H*W, 1, C)
        phi = -dt * div_v * hp   # [B,1,H,W] * [B,C,H,W] broadcast theo channel

        l_moment = self.deriv.moment_loss()
        return {"phi": phi, "div_v": div_v, "v": v,
                "dvx_dx": dvx_dx, "dvy_dy": dvy_dy, "l_moment": l_moment}

# %% [markdown]
# ### 2.7 C2-Enhancer — Criss-Cross Attention, Eq.(4)-(5)

# %%
"""
C2-Enhancer: criss-cross attention (Huang et al., CCNet [29]) used to enhance
h_t before the PDE operations, per Sec.II-A-1 and Fig.2.

For a feature map h_t in R^{C x W x H}:
  Q, K obtained via separate 1x1 convs with reduced channels C' < C
  V obtained via a separate 1x1 conv (channel count C, i.e. same as input)
  Horizontal path (attend along the row, i.e. varying x for fixed y):
      D_i,x = Q_x . K_i,x^T                         (Eq.4)
      A_i   = softmax(D_i,x)                        (softmax over D ONLY, not V)
      x'_h(x,y) = sum_i A_i V_i,x + x(x,y)           (Eq.5)
  Vertical path: the same operation, transposed (attend along the column).
  Fuse: h'_t = h_t + O_H + O_W   (Fig.2 "Addition" node combining both paths
        with the original feature; O_H, O_W are the attention-weighted sums
        WITHOUT re-adding the residual a second time -- see note below)

[AMBIGUOUS IN PAPER]: Eq.(5) as literally written adds x(x,y) inside each
directional branch, which would double the residual once H and W outputs are
summed together in Fig.2. We add the residual exactly once at the final fuse
step (h_t + O_H + O_W), which matches the original CCNet formulation this
module is based on and Fig.2's single "Addition" node. This is a deliberate,
documented deviation from a literal per-branch reading of Eq.(5); the
per-branch behaviour is retained via `residual_per_branch=True` if a literal
reproduction is desired.

Q/K/V conv weights are NOT shared between the horizontal and vertical paths
(Fig.2 draws Q_H/K_H/V_H and Q_W/K_W/V_W as separate convs).
"""
import torch
import torch.nn as nn
import torch.nn.functional as F


class _DirectionalCrissCross(nn.Module):
    """One directional (horizontal or vertical) branch of criss-cross attention."""

    def __init__(self, channels: int, reduced_channels: int):
        super().__init__()
        self.q_conv = nn.Conv2d(channels, reduced_channels, kernel_size=1)
        self.k_conv = nn.Conv2d(channels, reduced_channels, kernel_size=1)
        self.v_conv = nn.Conv2d(channels, channels, kernel_size=1)

    def forward(self, x: torch.Tensor, horizontal: bool) -> torch.Tensor:
        """
        x: [B,C,H,W]
        horizontal=True  -> attend across W (row-wise, fixed y)
        horizontal=False -> attend across H (column-wise, fixed x)
        Returns the attention-weighted aggregation O (same shape as x, NO
        residual added here -- residual is added once by the caller).
        """
        b, c, h, w = x.shape
        q = self.q_conv(x)   # [B,C',H,W]
        k = self.k_conv(x)
        v = self.v_conv(x)   # [B,C,H,W]

        if horizontal:
            # collapse batch*height, attend along width
            q = q.permute(0, 2, 3, 1).reshape(b * h, w, -1)          # [B*H, W, C']
            k = k.permute(0, 2, 3, 1).reshape(b * h, w, -1)          # [B*H, W, C']
            v = v.permute(0, 2, 3, 1).reshape(b * h, w, -1)          # [B*H, W, C]
            d = torch.bmm(q, k.transpose(1, 2))                      # Eq.4: D = Q K^T, [B*H, W, W]
            a = F.softmax(d, dim=-1)                                 # softmax on D only
            o = torch.bmm(a, v)                                      # Eq.5 (no residual yet)
            o = o.reshape(b, h, w, c).permute(0, 3, 1, 2)            # [B,C,H,W]
        else:
            q = q.permute(0, 3, 2, 1).reshape(b * w, h, -1)          # [B*W, H, C']
            k = k.permute(0, 3, 2, 1).reshape(b * w, h, -1)
            v = v.permute(0, 3, 2, 1).reshape(b * w, h, -1)
            d = torch.bmm(q, k.transpose(1, 2))                      # [B*W, H, H]
            a = F.softmax(d, dim=-1)
            o = torch.bmm(a, v)
            o = o.reshape(b, w, h, c).permute(0, 3, 2, 1)            # [B,C,H,W]
        return o


class C2Enhancer(nn.Module):
    def __init__(self, channels: int, reduction: int = 8, residual_per_branch: bool = False):
        super().__init__()
        c_reduced = max(channels // reduction, 1)
        self.horizontal = _DirectionalCrissCross(channels, c_reduced)
        self.vertical = _DirectionalCrissCross(channels, c_reduced)
        self.residual_per_branch = residual_per_branch

    def forward(self, h_t: torch.Tensor) -> torch.Tensor:
        o_h = self.horizontal(h_t, horizontal=True)
        o_w = self.vertical(h_t, horizontal=False)
        if self.residual_per_branch:
            # literal Eq.(5): residual added inside each branch, then fused
            return (o_h + h_t) + (o_w + h_t) - h_t  # avoids double-counting on fuse
        # default: single residual add at the fuse node (Fig.2), documented above
        return h_t + o_h + o_w

# %% [markdown]
# ### 2.8 InputAssimilation — Eq.(9)-(10)

# %%
"""
InputAssimilation: Kalman-gain-style correction, Eq.(9)-(10).

phi_{t+1} = hp(t) + phi(hp(t))              (Euler step of the advection term;
                                              this is the quantity Fig.2 labels
                                              "phi_{t+1}" feeding the Input
                                              Assimilation box)
K_t = tanh( Wh * phi_{t+1} + Wu * E(u_t) + bk )                       (Eq.10)
S(h,u) = K_t (.) [ E(u_t) - phi_{t+1} ]                               (Eq.9)
hp(t+1) = phi_{t+1} + S(h,u)
        = phi_{t+1} + K_t (.) (E(u_t) - phi_{t+1})

This final combination is not given as a standalone numbered equation but is
read directly off Fig.2's wiring: phi_{t+1} feeds both the Kalman-gain branch
and the "+" node that combines it with S(h,u) to yield h_{t+1}. It is the
natural Euler-integration reading of Mp(hp,u) = phi(hp) + S(hp,u) (Eq.3):
    hp(t+1) = hp(t) + Mp(hp,u) = [hp(t)+phi(hp(t))] + S = phi_{t+1} + S.

[AMBIGUOUS IN PAPER]: the text states "when Kt=0 ... only the portion
predicted by the PDE response; when Kt=1 ... driven entirely by the input
data", i.e. a {0,1} gate, but tanh's range is [-1,1]. We keep tanh exactly as
specified by Eq.(10) (do not silently swap for sigmoid) and flag the
inconsistency here, per the compact spec.
"""
import torch
import torch.nn as nn


class InputAssimilation(nn.Module):
    def __init__(self, channels: int):
        super().__init__()
        # Wh, Wu: learnable convs producing the Kalman-gain pre-activation.
        self.w_h = nn.Conv2d(channels, channels, kernel_size=3, padding=1, bias=False)
        self.w_u = nn.Conv2d(channels, channels, kernel_size=3, padding=1, bias=False)
        self.bk = nn.Parameter(torch.zeros(1, channels, 1, 1))
        self.tanh = nn.Tanh()

    def forward(self, phi_next: torch.Tensor, encoded_u: torch.Tensor):
        """
        phi_next:  phi_{t+1} = hp + phi(hp), [B,C,H,W]
        encoded_u: E(u_t), [B,C,H,W]
        Returns dict with K_t, S, and hp_{t+1}.
        """
        k_t = self.tanh(self.w_h(phi_next) + self.w_u(encoded_u) + self.bk)  # Eq.10
        residual = encoded_u - phi_next                                     # Eq.9 bracket
        s = k_t * residual                                                  # Eq.9 (Hadamard)
        hp_next = phi_next + s
        return {"k_t": k_t, "s": s, "hp_next": hp_next, "residual": residual}

# %% [markdown]
# ### 2.9 Phy-Predictor = AdvectionItems + InputAssimilation

# %%
"""
Phy-Predictor = AdvectionItems + InputAssimilation.
Implements MP(hp,u) = -(div V) hp + S(hp,u)   (Eq.3)
via the Euler-step reading documented in advection.py / assimilation.py.
"""
import torch
import torch.nn as nn


class PhyPredictor(nn.Module):
    def __init__(self, channels: int):
        super().__init__()
        self.advection = AdvectionItems(channels)
        self.assimilation = InputAssimilation(channels)

    def forward(self, hp: torch.Tensor, encoded_u: torch.Tensor):
        adv = self.advection(hp)                     # phi, div_v, v, l_moment
        phi_next = hp + adv["phi"]                    # Euler step -> "phi_{t+1}" in Fig.2
        assim = self.assimilation(phi_next, encoded_u)  # K_t, S, hp_next
        out = {**adv, **assim, "phi_next": phi_next}
        return out

# %% [markdown]
# ### 2.10 Full Phy-Unit = C2-Enhancer + Phy-Predictor

# %%
"""
Full Phy-Unit (Sec. II-A, Fig.2):
    hp(t) --C2-Enhancer--> enhanced hp --Phy-Predictor--> hp(t+1)
hr(t+1) = ConvLSTM(...) is handled separately in c2phynet.py.
"""
import torch
import torch.nn as nn


class PhyUnit(nn.Module):
    def __init__(self, channels: int):
        super().__init__()
        self.c2_enhancer = C2Enhancer(channels)
        self.phy_predictor = PhyPredictor(channels)
        self.norm = nn.GroupNorm(min(8, channels), channels)  # chặn hp trôi dạt qua 8 bước lặp

    def forward(self, hp, encoded_u):
        h_enhanced = self.c2_enhancer(hp)
        out = self.phy_predictor(h_enhanced, encoded_u)
        out["hp_next"] = self.norm(out["hp_next"])
        out["h_enhanced"] = h_enhanced
        return out

# %% [markdown]
# ## 3. Full C2PhyNet

# %%
"""
Full C2PhyNet (Sec.II, Fig.1):

For t = T-7 .. T:
    encoded = E(u_t)
    hp_{t+1} = PhyUnit(hp_t, encoded)          -> physical branch
    hr_{t+1} = ConvLSTM(hr_t, encoded)         -> potential-factor branch
    h_{t+1}  = hp_{t+1} + hr_{t+1}             -> h = h^p + h^r (Sec.II, para.3)
Decode the final h to get u_pred(T+1) = D(h_T+1).

h = hp + hr is treated as a learned disentanglement (soft, not a hard channel
split): both branches operate on the SAME full hidden-channel width and are
summed, exactly mirroring Fig.1's "(+)" fusion node.
"""
import torch
import torch.nn as nn


class C2PhyNet(nn.Module):
    def __init__(self, in_channels=1, out_channels=1, hidden_channels=64,
                 base_channels=32, num_stages=3):
        super().__init__()
        self.encoder = Encoder(in_channels, hidden_channels, base_channels, num_stages)
        self.decoder = Decoder(hidden_channels, base_channels, out_channels, num_stages)
        self.convlstm = ConvLSTMCell(hidden_channels, hidden_channels)
        self.phy_unit = PhyUnit(hidden_channels)
        self.hidden_channels = hidden_channels
        self.downsample_factor = 2 ** num_stages

    def _init_states(self, b, h, w, device):
        hp = torch.zeros(b, self.hidden_channels, h, w, device=device)
        hr, cr = self.convlstm.init_state(b, h, w, device)
        return hp, hr, cr

    def forward(self, x: torch.Tensor, return_intermediates: bool = False):
        """
        x: [B, T, C, H, W]   e.g. [2, 8, 1, 512, 512]
        Returns: u_pred [B, out_channels, H, W]  (single-step prediction of u_{T+1})
        """
        b, t_steps, c, h, w = x.shape
        hs = h // self.downsample_factor
        ws = w // self.downsample_factor
        hp, hr, cr = self._init_states(b, hs, ws, x.device)

        intermediates = []
        for t in range(t_steps):
            encoded = self.encoder(x[:, t])                 # E(u_t), [B,C_hidden,H/s,W/s]
            phy_out = self.phy_unit(hp, encoded)             # hp_{t+1} branch
            hp = phy_out["hp_next"]
            hr, cr = self.convlstm(encoded, (hr, cr))        # hr_{t+1} branch
            h_fused = hp + hr                                # h = h^p + h^r
            if return_intermediates:
                intermediates.append({**phy_out, "hr": hr, "h_fused": h_fused})

        u_pred = self.decoder(h_fused)
        if return_intermediates:
            return u_pred, intermediates
        return u_pred

    @torch.no_grad()
    def forecast(self, x: torch.Tensor, steps: int = 1):
        """
        Autoregressive multi-step inference: feed predicted frames back in.
        x: [B, T, C, H, W] seed window. Returns [B, steps, C, H, W].
        """
        self.eval()
        window = x.clone()
        preds = []
        for _ in range(steps):
            pred = self.forward(window)                     # [B,C,H,W]
            preds.append(pred)
            window = torch.cat([window[:, 1:], pred.unsqueeze(1)], dim=1)
        return torch.stack(preds, dim=1)

# %%
class C2PhyNetPretrained(nn.Module):
    """
    Giống C2PhyNet gốc, nhưng Encoder/Decoder được thay bằng PretrainedEncoder/Decoder
    (tái sử dụng trọng số CAE, 256 kênh). Vì Phy-Unit/ConvLSTM vốn được thiết kế cho
    hidden_channels nhỏ hơn (mặc định 64), ta chèn 2 conv 1x1 để nén 256 -> hidden_channels
    trước nhánh vật lý/ConvLSTM, rồi nén ngược lại trước khi decode ("bottleneck projection").
    """

    def __init__(self, in_channels: int = 1, out_channels: int = 1,
                 hidden_channels: int = 64, encoder_channels: int = 256):
        super().__init__()
        self.encoder = PretrainedEncoder(in_channels)
        self.decoder = PretrainedDecoder(out_channels)

        self.proj_down = nn.Conv2d(encoder_channels, hidden_channels, kernel_size=1)
        self.proj_up = nn.Conv2d(hidden_channels, encoder_channels, kernel_size=1)

        self.convlstm = ConvLSTMCell(hidden_channels, hidden_channels)
        self.phy_unit = PhyUnit(hidden_channels)

        self.hidden_channels = hidden_channels
        self.downsample_factor = 8  # 3 lần pool trong PretrainedEncoder

    def load_pretrained(self, encoder_path: str, decoder_path: str, device=None):
        device = device or next(self.parameters()).device
        miss_e, unexp_e = self.encoder.load_state_dict(
            torch.load(encoder_path, map_location=device), strict=False)
        miss_d, unexp_d = self.decoder.load_state_dict(
            torch.load(decoder_path, map_location=device), strict=False)
        print("Encoder - key mới (không nạp được từ file):", miss_e)
        print("Encoder - key thừa trong file (bị bỏ qua):", unexp_e)
        print("Decoder - key mới (không nạp được từ file):", miss_d)
        print("Decoder - key thừa trong file (bị bỏ qua):", unexp_d)

    def _init_states(self, b, h, w, device):
        hp = torch.zeros(b, self.hidden_channels, h, w, device=device)
        hr, cr = self.convlstm.init_state(b, h, w, device)
        return hp, hr, cr

    def forward(self, x: torch.Tensor, return_intermediates: bool = False):
        b, t_steps, c, h, w = x.shape
        hs, ws = h // self.downsample_factor, w // self.downsample_factor
        hp, hr, cr = self._init_states(b, hs, ws, x.device)

        intermediates = []
        for t in range(t_steps):
            encoded_full = self.encoder(x[:, t])       # [B,256,H/8,W/8]
            encoded = self.proj_down(encoded_full)      # [B,hidden,H/8,W/8]

            phy_out = self.phy_unit(hp, encoded)
            hp = phy_out["hp_next"]
            hr, cr = self.convlstm(encoded, (hr, cr))
            h_fused = hp + hr

            if return_intermediates:
                intermediates.append({**phy_out, "hr": hr, "h_fused": h_fused})

        h_fused_full = self.proj_up(h_fused)             # [B,256,H/8,W/8]
        u_pred = self.decoder(h_fused_full)

        if return_intermediates:
            return u_pred, intermediates
        return u_pred

    @torch.no_grad()
    def forecast(self, x: torch.Tensor, steps: int = 1):
        self.eval()
        window = x.clone()
        preds = []
        for _ in range(steps):
            pred = self.forward(window)
            preds.append(pred)
            window = torch.cat([window[:, 1:], pred.unsqueeze(1)], dim=1)
        return torch.stack(preds, dim=1)


def build_model(cfg: Config) -> nn.Module:
    m = cfg.model
    model = C2PhyNetPretrained(m.in_channels, m.out_channels, hidden_channels=m.hidden_channels)
    model.load_pretrained("encoder.pth", "decoder.pth")
    return model

# %%
import os

CHECKPOINT_LATEST_NAME = "checkpoint_latest.pt"
FREEZE_PRETRAINED_EPOCHS = 5   # số epoch đầu đóng băng encoder/decoder pretrained


def set_pretrained_trainable(model: nn.Module, trainable: bool):
    """Bật/tắt requires_grad cho phần encoder/decoder tái sử dụng từ CAE."""
    for p in model.encoder.parameters():
        p.requires_grad = trainable
    for p in model.decoder.parameters():
        p.requires_grad = trainable


def _build_optimizer(model: nn.Module, cfg: Config, frozen: bool):
    """frozen=True: chỉ optimize phần chưa đóng băng (Phy-Unit/ConvLSTM/proj).
    frozen=False: optimize toàn bộ, nhưng encoder/decoder pretrained dùng lr thấp hơn 10 lần."""
    if frozen:
        params = [p for p in model.parameters() if p.requires_grad]
        return torch.optim.Adam(params, lr=cfg.train.lr, weight_decay=cfg.train.weight_decay)

    pretrained_params = list(model.encoder.parameters()) + list(model.decoder.parameters())
    pretrained_ids = {id(p) for p in pretrained_params}
    other_params = [p for p in model.parameters() if id(p) not in pretrained_ids]
    return torch.optim.Adam(
        [
            {"params": other_params, "lr": cfg.train.lr},
            {"params": pretrained_params, "lr": cfg.train.lr * 0.1},
        ],
        weight_decay=cfg.train.weight_decay,
    )


def save_checkpoint(path, model, opt, scaler, epoch, epoch_finished, history, frozen):
    tmp_path = path + ".tmp"                                    # <-- THÊM: ghi ra file tạm trước
    torch.save({
        "model": model.state_dict(),
        "opt": opt.state_dict(),
        "scaler": scaler.state_dict(),
        "epoch": epoch,
        "epoch_finished": epoch_finished,   # True = đã xong trọn epoch này khi lưu
        "history": history,
        "frozen": frozen,
        "rng_state": torch.get_rng_state(),
        "cuda_rng_state": torch.cuda.get_rng_state_all() if torch.cuda.is_available() else None,
    }, tmp_path)                                                # <-- SỬA: path -> tmp_path
    os.replace(tmp_path, path)                                  # <-- THÊM: ghi xong mới thay file cũ


def load_checkpoint(path, model, opt, scaler, device):
    ckpt = torch.load(path, map_location=device)
    model.load_state_dict(ckpt["model"])
    opt.load_state_dict(ckpt["opt"])
    scaler.load_state_dict(ckpt["scaler"])
    torch.set_rng_state(ckpt["rng_state"].cpu())
    if ckpt["cuda_rng_state"] is not None and torch.cuda.is_available():
        cuda_rng_state = [t.cpu() for t in ckpt["cuda_rng_state"]]   # <-- SỬA: ép về CPU trước khi set
        torch.cuda.set_rng_state_all(cuda_rng_state)                  # <-- SỬA: dùng biến đã ép CPU
    start_epoch = ckpt["epoch"] if not ckpt["epoch_finished"] else ckpt["epoch"] + 1
    return start_epoch, ckpt["history"], ckpt["frozen"]

# %% [markdown]
# ## 4. Dataset (sliding-window, lazy-load Himawari B13)

# %%
from PIL import Image

"""
Sliding-window dataset over Himawari-8/9 B13 grayscale frames.
[u(t-7), ..., u(t)] -> u(t+1); supports arbitrary N_in / M (autoregressive
horizon is handled at the training-loop / inference level, not here).
Lazy-loads images (never materializes the full dataset in RAM).
"""
import os
from typing import List, Sequence
import numpy as np
import torch
from torch.utils.data import Dataset
from PIL import Image


class HimawariSequenceDataset(Dataset):
    def __init__(self, frame_paths: Sequence[str], n_in: int = 8, n_out: int = 1,
                 stride: int = 1, image_size: int = 512, normalize: bool = True):
        """
        frame_paths: chronologically ORDERED list of file paths (one per frame),
                     already restricted to a single, temporally-contiguous
                     typhoon sequence (do not mix sequences across typhoons).
        n_in:  number of input frames (paper default: 8)
        n_out: number of frames to predict (paper: single-step, n_out=1)
        """
        self.frame_paths: List[str] = list(frame_paths)
        self.n_in = n_in
        self.n_out = n_out
        self.stride = stride
        self.image_size = image_size
        self.normalize = normalize

        window = n_in + n_out
        self.starts = list(range(0, len(self.frame_paths) - window + 1, stride))
        if not self.starts:
            raise ValueError(
                f"Sequence of length {len(self.frame_paths)} too short for "
                f"window n_in+n_out={window}"
            )

    def __len__(self):
        return len(self.starts)

    def _load(self, path: str) -> torch.Tensor:
        img = Image.open(path)
        if img.size != (self.image_size, self.image_size):
            img = img.resize((self.image_size, self.image_size), Image.BILINEAR)
        arr = np.asarray(img, dtype=np.float32)
        if arr.ndim == 3:
            arr = arr[..., 0]  # collapse to single channel if needed
        if self.normalize:
            arr = arr / 255.0 if arr.max() > 1.0 else arr
        return torch.from_numpy(arr).unsqueeze(0)  # [1,H,W]

    def __getitem__(self, idx: int):
        start = self.starts[idx]
        in_paths = self.frame_paths[start: start + self.n_in]
        out_paths = self.frame_paths[start + self.n_in: start + self.n_in + self.n_out]

        x = torch.stack([self._load(p) for p in in_paths], dim=0)   # [n_in, 1, H, W]
        y = torch.stack([self._load(p) for p in out_paths], dim=0)  # [n_out, 1, H, W]
        if self.n_out == 1:
            y = y[0]  # [1,H,W] for single-step prediction
        return x, y


import re
from datetime import datetime
import numpy as np
from torch.utils.data import ConcatDataset

_TS_RE = re.compile(r"^(\d{10})-")  # 10 ký tự đầu tên file = YYYYMMDDHH

def parse_timestamp(filename: str) -> datetime:
    m = _TS_RE.match(filename)
    if not m:
        raise ValueError(f"Không tìm thấy timestamp ở đầu tên file: {filename}")
    return datetime.strptime(m.group(1), "%Y%m%d%H")


def build_typhoon_sequences(root_dir: str, n_in: int = 8, n_out: int = 1,
                             stride: int = 1, image_size: int = 512,
                             gap_factor: float = 1.5,
                             max_median_gap_hours: float = None,
                             year_range: tuple = None,
                             extensions=(".png", ".tif", ".jpg")):
    typhoon_dirs = sorted(
        d for d in os.listdir(root_dir)
        if os.path.isdir(os.path.join(root_dir, d))
    )

    datasets = []
    kept_typhoons = 0
    dropped = []
    total_frames = total_sequences = 0

    for tid in typhoon_dirs:
        if year_range is not None:
            try:
                y = int(tid[:4])
            except ValueError:
                dropped.append((tid, 0, "tên thư mục không parse được năm"))
                continue
            if not (year_range[0] <= y <= year_range[1]):
                continue

        tdir = os.path.join(root_dir, tid)
        files = [f for f in os.listdir(tdir) if f.lower().endswith(extensions)]

        by_ts = {}
        n_dup = 0
        for f in files:
            try:
                ts = parse_timestamp(f)
            except ValueError:
                continue
            if ts in by_ts:
                n_dup += 1
                continue
            by_ts[ts] = os.path.join(tdir, f)

        items = sorted(by_ts.items(), key=lambda x: x[0])
        if len(items) < n_in + n_out:
            dropped.append((tid, len(items), "quá ít ảnh"))
            continue

        gaps = np.array([
            (t2 - t1).total_seconds() / 3600.0
            for (t1, _), (t2, _) in zip(items, items[1:])
        ])
        typical_gap = float(np.median(gaps)) if len(gaps) else 1.0

        if max_median_gap_hours is not None and typical_gap > max_median_gap_hours:
            dropped.append((tid, len(items),
                             f"nhịp quan trắc quá thưa (~{typical_gap:.1f}h > "
                             f"ngưỡng {max_median_gap_hours}h)"))
            continue

        threshold = max(typical_gap * gap_factor, typical_gap + 1e-6)

        chunk, chunks = [items[0]], []
        for (t_prev, _), (t, p) in zip(items, items[1:]):
            gap = (t - t_prev).total_seconds() / 3600.0
            if gap <= threshold:
                chunk.append((t, p))
            else:
                chunks.append(chunk)
                chunk = [(t, p)]
        chunks.append(chunk)

        n_seq = 0
        for c in chunks:
            paths = [p for _, p in c]
            if len(paths) < n_in + n_out:
                continue
            ds = HimawariSequenceDataset(paths, n_in=n_in, n_out=n_out,
                                          stride=stride, image_size=image_size)
            datasets.append(ds)
            total_frames += len(paths)
            total_sequences += len(ds)
            n_seq += len(ds)

        if n_seq == 0:
            dropped.append((tid, len(items),
                             f"không có đoạn liên tục >= {n_in+n_out} khung "
                             f"(nhịp ~{typical_gap:.1f}h, {n_dup} bản trùng giờ)"))
        else:
            kept_typhoons += 1

    print(f"Quét {len(typhoon_dirs)} thư mục cơn bão trong '{root_dir}'"
          + (f" (lọc năm {year_range[0]}-{year_range[1]})" if year_range else "")
          + (f" (loại nhịp quan trắc > {max_median_gap_hours}h)" if max_median_gap_hours else ""))
    print(f"  -> giữ {kept_typhoons} cơn bão, {len(datasets)} đoạn liên tục, "
          f"{total_frames} khung ảnh, {total_sequences} sequence {n_in}-vào/{n_out}-ra")
    print(f"  -> loại {len(dropped)} cơn bão không đủ dữ liệu")
    for tid, n, reason in dropped[:10]:
        print(f"     - {tid}: {n} ảnh — {reason}")
    if len(dropped) > 10:
        print(f"     ... và {len(dropped) - 10} cơn bão khác")

    if not datasets:
        raise RuntimeError("Không cơn bão nào đủ dữ liệu — kiểm tra lại root_dir/n_in/n_out/year_range/max_median_gap_hours.")

    return ConcatDataset(datasets)

# %% [markdown]
# ## 5. Physics / Moment Loss

# %%
"""
Ltotal = Lpred + lambda_moment * Lmoment          (Sec. II text near Eq.8)
Lpred  = MSE(pred, target)
Optional SSIM / reconstruction terms can be added but the core physics/moment
logic (Lmoment from DerivativeOperator) must remain.
"""
import torch
import torch.nn as nn
import torch.nn.functional as F


def _ssim(pred: torch.Tensor, target: torch.Tensor, window_size: int = 11, c1=0.01 ** 2, c2=0.03 ** 2):
    """Lightweight single-scale SSIM (optional term, not part of the paper's core loss)."""
    pad = window_size // 2
    mu_p = F.avg_pool2d(pred, window_size, 1, pad)
    mu_t = F.avg_pool2d(target, window_size, 1, pad)
    mu_p2, mu_t2, mu_pt = mu_p ** 2, mu_t ** 2, mu_p * mu_t
    sigma_p2 = F.avg_pool2d(pred * pred, window_size, 1, pad) - mu_p2
    sigma_t2 = F.avg_pool2d(target * target, window_size, 1, pad) - mu_t2
    sigma_pt = F.avg_pool2d(pred * target, window_size, 1, pad) - mu_pt
    ssim_map = ((2 * mu_pt + c1) * (2 * sigma_pt + c2)) / ((mu_p2 + mu_t2 + c1) * (sigma_p2 + sigma_t2 + c2))
    return ssim_map.mean()


class PhysicsLoss(nn.Module):
    def __init__(self, lambda_moment: float = 1.0, use_ssim: bool = False, lambda_ssim: float = 0.0):
        super().__init__()
        self.lambda_moment = lambda_moment
        self.use_ssim = use_ssim
        self.lambda_ssim = lambda_ssim

    def forward(self, pred: torch.Tensor, target: torch.Tensor, l_moment: torch.Tensor):
        l_pred = F.mse_loss(pred, target)
        total = l_pred + self.lambda_moment * l_moment
        logs = {"l_pred": l_pred.detach(), "l_moment": l_moment.detach()}
        if self.use_ssim:
            l_ssim = 1.0 - _ssim(pred, target)
            total = total + self.lambda_ssim * l_ssim
            logs["l_ssim"] = l_ssim.detach()
        logs["l_total"] = total.detach()
        return total, logs

# %%
import time
import torch
import torch.nn.functional as F


@torch.no_grad()
def psnr(pred: torch.Tensor, target: torch.Tensor, max_val: float = 1.0) -> torch.Tensor:
    mse = F.mse_loss(pred, target)
    return 10 * torch.log10(max_val ** 2 / (mse + 1e-12))


def _fmt_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


@torch.no_grad()
def _batch_metrics(pred: torch.Tensor, target: torch.Tensor) -> dict:
    """MSE/MAE/SSIM/PSNR trung bình của 1 batch — dùng để hiện trong thanh tiến trình mỗi epoch."""
    return {
        "mse": F.mse_loss(pred, target).item(),
        "mae": F.l1_loss(pred, target).item(),
        "ssim": _ssim(pred, target).item(),
        "psnr": psnr(pred, target).item(),
    }

# %% [markdown]
# ## 6. Unit test / kiểm tra shape
# Chạy ngay bằng tensor ngẫu nhiên `torch.randn(2, 8, 1, H, W)` — xác nhận mọi module
# khớp shape theo đúng spec, không cần dataset thật. Có một bước nhỏ (64×64) để chạy
# nhanh trên CPU, và một bước đúng độ phân giải gốc (512×512) theo compact prompt.

# %%
def run_shape_checks(h=64, w=64, run_full_512=False, device=DEVICE):
    torch.manual_seed(0)

    # -- SCSE --
    x = torch.randn(2, 32, 16, 16, device=device)
    assert SCSEAttention(32).to(device)(x).shape == x.shape

    # -- Encoder / Decoder --
    enc = Encoder(1, 64, 32, 3).to(device)
    dec = Decoder(64, 32, 1, 3).to(device)
    xin = torch.randn(2, 1, h, w, device=device)
    e = enc(xin)
    assert e.shape == (2, 64, h // 8, w // 8), e.shape
    out = dec(e)
    assert out.shape == (2, 1, h, w), out.shape

    # -- ConvLSTM --
    cell = ConvLSTMCell(64, 64).to(device)
    xc = torch.randn(2, 64, 8, 8, device=device)
    h0, c0 = cell.init_state(2, 8, 8, device)
    hn, cn = cell(xc, (h0, c0))
    assert hn.shape == cn.shape == (2, 64, 8, 8)

    # -- C2-Enhancer --
    c2 = C2Enhancer(64).to(device)
    xh = torch.randn(2, 64, 8, 8, device=device)
    assert c2(xh).shape == xh.shape

    # -- AdvectionItems --
    adv = AdvectionItems(64).to(device)
    hp = torch.randn(2, 64, 8, 8, device=device)
    out_adv = adv(hp)
    assert out_adv["phi"].shape == hp.shape
    assert out_adv["div_v"].shape == (2, 1, 8, 8)
    assert out_adv["l_moment"].dim() == 0

    # -- InputAssimilation --
    assim = InputAssimilation(64).to(device)
    e_u = torch.randn(2, 64, 8, 8, device=device)
    out_assim = assim(hp, e_u)
    assert out_assim["hp_next"].shape == hp.shape

    # -- PhyPredictor / PhyUnit --
    pp = PhyPredictor(64).to(device)
    out_pp = pp(hp, e_u)
    assert out_pp["hp_next"].shape == hp.shape
    pu = PhyUnit(64).to(device)
    out_pu = pu(hp, e_u)
    assert out_pu["hp_next"].shape == hp.shape

    # -- Full model --
    model = C2PhyNet(1, 1, 64, 32, 3).to(device)
    xseq = torch.randn(2, 8, 1, h, w, device=device)
    pred, inter = model(xseq, return_intermediates=True)
    assert pred.shape == (2, 1, h, w), pred.shape
    last = inter[-1]
    assert last["hp_next"].shape == last["hr"].shape == last["h_fused"].shape

    # -- Autoregressive forecast --
    fc = model.forecast(xseq[:1], steps=3)
    assert fc.shape == (1, 3, 1, h, w), fc.shape

    print(f"All shape checks passed at {h}x{w}.")

    if run_full_512:
        print("Running full spec check: x = torch.randn(2,8,1,512,512) ...")
        run_shape_checks(512, 512, run_full_512=False, device=device)




# %%
# Bước đúng như compact spec: x = torch.randn(2, 8, 1, 512, 512) -> pred [2, 1, 512, 512]
# Cảnh báo: chậm trên CPU (vài chục giây tới vài phút); nhanh trên GPU RTX 4070 Ti Super.
# [ĐÃ TẮT] run_shape_checks(512, 512, run_full_512=False)

# %%
import torch

def inspect_phi_kt(model, x, device=None):
    """
    x: [B, T, C, H, W] — ví dụ torch.randn(2, 8, 1, 64, 64) hoặc batch thật từ dataloader.
    In ra thống kê (min/max/mean/std, có NaN/Inf hay không) của phi_next và k_t
    tại MỖI bước thời gian t = 0..T-1, để xem giá trị bùng nổ từ bước nào.
    """
    device = device or next(model.parameters()).device
    model = model.to(device).eval()
    x = x.to(device)

    with torch.no_grad():
        u_pred, intermediates = model(x, return_intermediates=True)

    print(f"{'t':>2} | {'phi_next min':>14} {'phi_next max':>14} {'phi_next mean':>14} {'has_nan/inf':>12} "
          f"| {'k_t min':>10} {'k_t max':>10} {'k_t mean':>10} {'has_nan/inf':>12}")
    print("-" * 110)

    for t, inter in enumerate(intermediates):
        phi_next = inter["phi_next"]
        k_t = inter["k_t"]

        phi_bad = torch.isnan(phi_next).any() or torch.isinf(phi_next).any()
        kt_bad = torch.isnan(k_t).any() or torch.isinf(k_t).any()

        print(f"{t:>2} | {phi_next.min().item():14.4f} {phi_next.max().item():14.4f} "
              f"{phi_next.mean().item():14.4f} {str(phi_bad.item()):>12} "
              f"| {k_t.min().item():10.4f} {k_t.max().item():10.4f} "
              f"{k_t.mean().item():10.4f} {str(kt_bad.item()):>12}")

        if phi_bad or kt_bad:
            print(f"    -> NaN/Inf xuất hiện lần đầu ở bước t={t}")
            break

    return intermediates


# Chạy thử với input random 8 khung hình (không cần dataset thật):
# [ĐÃ TẮT] model = C2PhyNet(1, 1, 64, 32, 3)
# [ĐÃ TẮT] x_test = torch.randn(2, 8, 1, 64, 64)   # đổi 64,64 thành kích thước thật của bạn nếu cần
# [ĐÃ TẮT] _ = inspect_phi_kt(model, x_test)

# %% [markdown]
# ## 7. Training
# Chỉnh `cfg.data.root_dir` trỏ tới thư mục ảnh Himawari-8/9 B13 (grayscale, đặt tên file
# theo thứ tự thời gian, ví dụ `YYYYMMDDHH.png`) trước khi chạy. Hỗ trợ AMP
# (`torch.amp.autocast`/`GradScaler`), gradient accumulation, gradient clipping, checkpoint.

# %%
import time
from torch.amp import autocast, GradScaler
from torch.utils.data import DataLoader
from tqdm.auto import tqdm


def train(cfg: Config, resume: bool = True, checkpoint_every_steps: int = 200,
          freeze_epochs: int = FREEZE_PRETRAINED_EPOCHS):
    device = torch.device(cfg.device if torch.cuda.is_available() else "cpu")
    torch.manual_seed(cfg.seed)

    dataset = build_typhoon_sequences(
        cfg.data.root_dir, cfg.data.n_in, cfg.data.n_out, cfg.data.stride,
        cfg.data.image_size, max_median_gap_hours=3.0,
    )
    loader = DataLoader(
        dataset, batch_size=cfg.train.batch_size, shuffle=True,
        num_workers=cfg.data.num_workers, pin_memory=True,
        persistent_workers=cfg.data.num_workers > 0,
    )

    model = build_model(cfg).to(device)
    criterion = PhysicsLoss(cfg.train.lambda_moment, cfg.train.use_ssim, cfg.train.lambda_ssim)

    os.makedirs(cfg.train.checkpoint_dir, exist_ok=True)
    ckpt_path = os.path.join(cfg.train.checkpoint_dir, CHECKPOINT_LATEST_NAME)

    start_epoch = 0
    history = []
    frozen = freeze_epochs > 0

    set_pretrained_trainable(model, not frozen)
    opt = _build_optimizer(model, cfg, frozen)
    scaler = GradScaler(enabled=cfg.train.amp)

    if resume and os.path.isfile(ckpt_path):
        # đọc trước "frozen" đã lưu để dựng đúng optimizer (số param group) rồi mới load_state_dict
        raw = torch.load(ckpt_path, map_location=device)
        frozen = raw["frozen"]
        set_pretrained_trainable(model, not frozen)
        opt = _build_optimizer(model, cfg, frozen)
        start_epoch, history, frozen = load_checkpoint(ckpt_path, model, opt, scaler, device)
        print(f"Đã resume từ checkpoint: tiếp tục từ epoch {start_epoch + 1}, frozen={frozen}")

    for epoch in range(start_epoch, cfg.train.epochs):
        if frozen and epoch >= freeze_epochs:
            frozen = False
            set_pretrained_trainable(model, True)
            opt = _build_optimizer(model, cfg, frozen)
            print(f"Epoch {epoch + 1}: mở khóa encoder/decoder pretrained (lr encoder/decoder = lr x0.1)")

        model.train()
        opt.zero_grad(set_to_none=True)

        running = {"mse": 0.0, "mae": 0.0, "ssim": 0.0, "psnr": 0.0, "l_total": 0.0}
        n_batches = len(loader)
        epoch_start = time.time()

        pbar = tqdm(loader, total=n_batches, leave=False,
                    desc=f"Epoch {epoch + 1}/{cfg.train.epochs}")
        for i, (x, y) in enumerate(pbar):
            x, y = x.to(device, non_blocking=True), y.to(device, non_blocking=True)
            with autocast(device_type=device.type, enabled=cfg.train.amp):
                pred, inter = model(x, return_intermediates=True)
                l_moment = inter[-1]["l_moment"]
                loss, logs = criterion(pred, y, l_moment)
                loss = loss / cfg.train.grad_accum_steps

            scaler.scale(loss).backward()

            if (i + 1) % cfg.train.grad_accum_steps == 0:
                scaler.unscale_(opt)
                torch.nn.utils.clip_grad_norm_(model.parameters(), cfg.train.grad_clip_norm)
                scaler.step(opt)
                scaler.update()
                opt.zero_grad(set_to_none=True)

            with torch.no_grad():
                bm = _batch_metrics(pred.float(), y.float())
            bsz = x.size(0)
            for k in ("mse", "mae", "ssim", "psnr"):
                running[k] += bm[k] * bsz
            running["l_total"] += logs["l_total"].item() * bsz

            # KHÔNG print() trong vòng lặp — chỉ set_postfix, để tqdm giữ đúng 1 dòng/epoch
            pbar.set_postfix({
                "loss": f"{logs['l_total'].item():.4f}",
                "mse": f"{bm['mse']:.4f}",
                "psnr": f"{bm['psnr']:.2f}",
                "mae": f"{bm['mae']:.4f}",
                "ssim": f"{bm['ssim']:.4f}",
            })

            global_step = epoch * n_batches + i + 1
            if global_step % checkpoint_every_steps == 0:
                save_checkpoint(ckpt_path, model, opt, scaler, epoch,
                                 epoch_finished=False, history=history, frozen=frozen)

        pbar.close()

        n_samples = len(dataset)
        epoch_time = time.time() - epoch_start
        epoch_metrics = {k: v / n_samples for k, v in running.items()}
        epoch_metrics["epoch"] = epoch
        epoch_metrics["time_sec"] = epoch_time
        history.append(epoch_metrics)

        remaining = epoch_time * (cfg.train.epochs - epoch - 1)
        print(
            f"Epoch {epoch + 1}/{cfg.train.epochs} | "
            f"loss={epoch_metrics['l_total']:.4f} mse={epoch_metrics['mse']:.4f} "
            f"mae={epoch_metrics['mae']:.4f} ssim={epoch_metrics['ssim']:.4f} "
            f"psnr={epoch_metrics['psnr']:.2f} | "
            f"{_fmt_time(epoch_time)}/epoch, còn lại ~{_fmt_time(remaining)}"
        )

        save_checkpoint(ckpt_path, model, opt, scaler, epoch,
                         epoch_finished=True, history=history, frozen=frozen)

    return model, history

# %%
import torch

def inspect_phi_kt(model, x, device=None):
    """
    x: [B, T, C, H, W] — ví dụ torch.randn(2, 8, 1, 64, 64) hoặc batch thật từ dataloader.
    In ra thống kê (min/max/mean/std, có NaN/Inf hay không) của phi_next và k_t
    tại MỖI bước thời gian t = 0..T-1, để xem giá trị bùng nổ từ bước nào.
    """
    device = device or next(model.parameters()).device
    model = model.to(device).eval()
    x = x.to(device)

    with torch.no_grad():
        u_pred, intermediates = model(x, return_intermediates=True)

    print(f"{'t':>2} | {'phi_next min':>14} {'phi_next max':>14} {'phi_next mean':>14} {'has_nan/inf':>12} "
          f"| {'k_t min':>10} {'k_t max':>10} {'k_t mean':>10} {'has_nan/inf':>12}")
    print("-" * 110)

    for t, inter in enumerate(intermediates):
        phi_next = inter["phi_next"]
        k_t = inter["k_t"]

        phi_bad = torch.isnan(phi_next).any() or torch.isinf(phi_next).any()
        kt_bad = torch.isnan(k_t).any() or torch.isinf(k_t).any()

        print(f"{t:>2} | {phi_next.min().item():14.4f} {phi_next.max().item():14.4f} "
              f"{phi_next.mean().item():14.4f} {str(phi_bad.item()):>12} "
              f"| {k_t.min().item():10.4f} {k_t.max().item():10.4f} "
              f"{k_t.mean().item():10.4f} {str(kt_bad.item()):>12}")

        if phi_bad or kt_bad:
            print(f"    -> NaN/Inf xuất hiện lần đầu ở bước t={t}")
            break

    return intermediates


# Chạy thử với input random 8 khung hình (không cần dataset thật):
# [ĐÃ TẮT] model = C2PhyNet(1, 1, 64, 32, 3)
# [ĐÃ TẮT] x_test = torch.randn(2, 8, 1, 64, 64)   # đổi 64,64 thành kích thước thật của bạn nếu cần
# [ĐÃ TẮT] _ = inspect_phi_kt(model, x_test)

# %%
# Ví dụ gọi training (bỏ comment và chỉnh root_dir để chạy thật):
# cfg = Config()
# cfg.data.root_dir = "/path/to/himawari_b13_frames"
# cfg.train.epochs = 100
# model = train(cfg)

# %%
import os
# [ĐÃ TẮT] print(os.getcwd())
# [ĐÃ TẮT] print(os.path.exists("encoder.pth"), os.path.exists("decoder.pth"))

# %%




# %% [markdown]
# ## 8. Evaluation (MSE / MAE / SSIM / PSNR)

# %%
@torch.no_grad()
def psnr(pred, target, max_val=1.0):
    mse = F.mse_loss(pred, target)
    return 10 * torch.log10(max_val ** 2 / (mse + 1e-12))


@torch.no_grad()
def evaluate(cfg: Config, model: nn.Module):
    device = torch.device(cfg.device if torch.cuda.is_available() else "cpu")
    model = model.to(device).eval()

    paths = build_sequence_paths(cfg.data.root_dir)
    dataset = HimawariSequenceDataset(paths, cfg.data.n_in, cfg.data.n_out,
                                       cfg.data.stride, cfg.data.image_size)
    loader = DataLoader(dataset, batch_size=cfg.train.batch_size, shuffle=False,
                         num_workers=cfg.data.num_workers)

    totals = {"mse": 0.0, "mae": 0.0, "ssim": 0.0, "psnr": 0.0}
    n = 0
    for x, y in loader:
        x, y = x.to(device), y.to(device)
        pred = model(x)
        b = x.shape[0]
        totals["mse"] += F.mse_loss(pred, y).item() * b
        totals["mae"] += F.l1_loss(pred, y).item() * b
        totals["ssim"] += _ssim(pred, y).item() * b
        totals["psnr"] += psnr(pred, y).item() * b
        n += b
    return {k: v / n for k, v in totals.items()}

# Ví dụ: metrics = evaluate(cfg, model)

# %%
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# palette categorical cố định (xem dataviz skill) — mỗi đại lượng gắn 1 màu, không đổi theo thứ hạng
COLOR_SURFACE   = "#fcfcfb"
COLOR_PRIMARY   = "#0b0b0b"
COLOR_SECONDARY = "#52514e"
COLOR_MUTED     = "#898781"
COLOR_GRID      = "#e1e0d9"
COLOR_BASELINE  = "#c3c2b7"

SERIES = {
    "l_pred":   "#2a78d6",  # blue
    "l_moment": "#eb6834",  # orange
    "l_total":  "#008300",  # green
    "mse":      "#2a78d6",
    "mae":      "#eb6834",
    "ssim":     "#1baf7a",  # aqua
    "psnr":     "#4a3aa7",  # violet
    "time":     "#e34948",  # red
}


def plot_training_history(history: list):
    """
    Vẽ lại diễn biến training từ `history` (list các dict theo epoch, trả về
    từ train(cfg) ở mục 7). Mỗi đại lượng có scale khác nhau nên tách thành
    các panel riêng (small multiples) thay vì gộp chung 1 trục.
    """
    if not history:
        print("history rỗng — chạy `model, history = train(cfg)` ở mục 7 trước.")
        return

    epochs = [h["epoch"] + 1 for h in history]  # 1-indexed cho dễ đọc

    fig, axes = plt.subplots(2, 3, figsize=(15, 8), facecolor=COLOR_SURFACE)
    fig.suptitle("Diễn biến quá trình training C2PhyNet", fontsize=14,
                 color=COLOR_PRIMARY, fontweight="bold")

    def _style_ax(ax, title, ylabel):
        ax.set_facecolor(COLOR_SURFACE)
        ax.set_title(title, color=COLOR_PRIMARY, fontsize=11, loc="left")
        ax.set_xlabel("Epoch", color=COLOR_SECONDARY, fontsize=9)
        ax.set_ylabel(ylabel, color=COLOR_SECONDARY, fontsize=9)
        ax.grid(True, color=COLOR_GRID, linewidth=0.8, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(COLOR_BASELINE)
        ax.spines["bottom"].set_color(COLOR_BASELINE)
        ax.tick_params(colors=COLOR_MUTED, labelsize=8)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    def _line(ax, y_key, color, label=None):
        y = [h[y_key] for h in history]
        ax.plot(epochs, y, color=color, linewidth=2, marker="o", markersize=4,
                 label=label, zorder=3)
        ax.annotate(f"{y[-1]:.4g}", xy=(epochs[-1], y[-1]),
                    xytext=(6, 0), textcoords="offset points",
                    fontsize=8, color=color, va="center")
        return y

    # 1) Loss breakdown — 1 trục log vì l_moment thường nhỏ hơn l_pred nhiều bậc
    ax = axes[0, 0]
    _line(ax, "l_pred", SERIES["l_pred"], "L_pred (MSE)")
    _line(ax, "l_moment", SERIES["l_moment"], "L_moment")
    _line(ax, "l_total", SERIES["l_total"], "L_total")
    ax.set_yscale("log")
    _style_ax(ax, "Loss theo epoch (log scale)", "Loss")
    ax.legend(frameon=False, fontsize=8, labelcolor=COLOR_SECONDARY)

    ax = axes[0, 1]; _line(ax, "mse", SERIES["mse"]); _style_ax(ax, "MSE theo epoch", "MSE")
    ax = axes[0, 2]; _line(ax, "mae", SERIES["mae"]); _style_ax(ax, "MAE theo epoch", "MAE")

    ax = axes[1, 0]
    _line(ax, "ssim", SERIES["ssim"])
    ax.set_ylim(0, 1)
    _style_ax(ax, "SSIM theo epoch (càng cao càng tốt)", "SSIM")

    ax = axes[1, 1]
    _line(ax, "psnr", SERIES["psnr"])
    _style_ax(ax, "PSNR theo epoch (càng cao càng tốt, dB)", "PSNR (dB)")

    ax = axes[1, 2]
    times = [h["time_sec"] for h in history]
    ax.bar(epochs, times, color=SERIES["time"], width=0.6, zorder=3)
    avg_t = sum(times) / len(times)
    ax.axhline(avg_t, color=COLOR_MUTED, linewidth=1, linestyle="--", zorder=2)
    ax.annotate(f"avg={avg_t:.1f}s", xy=(epochs[-1], avg_t),
                xytext=(6, 4), textcoords="offset points",
                fontsize=8, color=COLOR_MUTED)
    _style_ax(ax, "Thời gian train mỗi epoch", "Giây")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()
    return fig


# Gọi sau khi đã có: model, history = train(cfg)
# plot_training_history(history)

# %%
# [ĐÃ TẮT] print(type(model))

# %%
# [ĐÃ TẮT] model, history = model
# [ĐÃ TẮT] print(len(history))   # kỳ vọng: 5

# %%
# [ĐÃ TẮT] fig = plot_training_history(history)

# %% [markdown]
# ## 9. Autoregressive Inference

# %%
@torch.no_grad()
def run_inference(cfg: Config, model: nn.Module, steps: int = 1):
    device = torch.device(cfg.device if torch.cuda.is_available() else "cpu")
    model = model.to(device).eval()

    paths = build_sequence_paths(cfg.data.root_dir)
    dataset = HimawariSequenceDataset(paths, cfg.data.n_in, cfg.data.n_out,
                                       cfg.data.stride, cfg.data.image_size)
    x, _ = dataset[0]
    x = x.unsqueeze(0).to(device)
    preds = model.forecast(x, steps=steps)
    return preds.cpu()

# Ví dụ: preds = run_inference(cfg, model, steps=6)  # forecast 6 bước tiếp theo


# %%
# ===== Điểm chạy chính khi chạy bằng: python train_ddp.py =====
if __name__ == "__main__":
    cfg = Config()
    cfg.data.root_dir = r"E:\minhan_storm_trajectory_final_v2\digital_typhoon_datasets\image_png"
    cfg.data.num_workers = 4
    cfg.train.epochs = 100
    cfg.train.batch_size = 32
    cfg.train.checkpoint_dir = "checkpoints"
    train(cfg, resume=True, checkpoint_every_steps=200)
