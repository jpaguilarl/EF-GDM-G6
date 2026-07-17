---
name: NYC TLC Data Portal
colors:
  yellow-taxi: '#F3C613'
  green-taxi: '#2A9D8F'
  purple-apps: '#5F9EA0'
  purple-apps-dark: '#4B0082'
  alert: '#E63946'
  success: '#2A9D8F'
  surface: '#fbf9f8'
  surface-dim: '#dcd9d9'
  surface-bright: '#fbf9f8'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f6f3f2'
  surface-container: '#f0eded'
  surface-container-high: '#eae8e7'
  surface-container-highest: '#e4e2e1'
  on-surface: '#1b1c1c'
  on-surface-variant: '#5c403b'
  inverse-surface: '#303030'
  inverse-on-surface: '#f3f0f0'
  outline: '#916f6a'
  outline-variant: '#e6bdb7'
  surface-tint: '#be0e0b'
  primary: '#a00003'
  on-primary: '#ffffff'
  primary-container: '#c81912'
  on-primary-container: '#ffdbd6'
  inverse-primary: '#ffb4a9'
  secondary: '#415f8e'
  on-secondary: '#ffffff'
  secondary-container: '#adcaff'
  on-secondary-container: '#375583'
  tertiary: '#004aa0'
  on-tertiary: '#ffffff'
  tertiary-container: '#0061ce'
  on-tertiary-container: '#d9e3ff'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#ffdad5'
  primary-fixed-dim: '#ffb4a9'
  on-primary-fixed: '#410000'
  on-primary-fixed-variant: '#930002'
  secondary-fixed: '#d6e3ff'
  secondary-fixed-dim: '#aac7fd'
  on-secondary-fixed: '#001b3d'
  on-secondary-fixed-variant: '#284775'
  tertiary-fixed: '#d8e2ff'
  tertiary-fixed-dim: '#adc6ff'
  on-tertiary-fixed: '#001a41'
  on-tertiary-fixed-variant: '#004494'
  background: '#fbf9f8'
  on-background: '#1b1c1c'
  surface-variant: '#e4e2e1'
  surface-muted: '#F3F4F6'
  border-subtle: '#E5E7EB'
  peru-red-dark: '#BF091E'
typography:
  display-lg:
    fontFamily: Public Sans
    fontSize: 48px
    fontWeight: '700'
    lineHeight: 56px
    letterSpacing: -0.02em
  headline-lg:
    fontFamily: Public Sans
    fontSize: 32px
    fontWeight: '700'
    lineHeight: 40px
  headline-lg-mobile:
    fontFamily: Public Sans
    fontSize: 24px
    fontWeight: '700'
    lineHeight: 32px
  headline-md:
    fontFamily: Public Sans
    fontSize: 24px
    fontWeight: '600'
    lineHeight: 32px
  body-lg:
    fontFamily: Public Sans
    fontSize: 18px
    fontWeight: '400'
    lineHeight: 28px
  body-md:
    fontFamily: Public Sans
    fontSize: 16px
    fontWeight: '400'
    lineHeight: 24px
  label-md:
    fontFamily: Public Sans
    fontSize: 14px
    fontWeight: '600'
    lineHeight: 20px
    letterSpacing: 0.01em
  caption:
    fontFamily: Public Sans
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 16px
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  base: 4px
  container-max-width: 1200px
  gutter: 24px
  margin-mobile: 16px
  stack-sm: 8px
  stack-md: 16px
  stack-lg: 32px
---

## Brand & Style

The design system is engineered for public trust, transparency, and administrative clarity. It shifts from a high-tech "command center" aesthetic to a **document-centric official portal style**, prioritizing accessibility and the efficient delivery of information.

The aesthetic is rooted in **Modern Minimalism with a Corporate/Institutional focus**. It utilizes heavy whitespace, a strict adherence to a grid, and a high-contrast palette to ensure information remains the primary focus. The visual language evokes a sense of order, reliability, and governmental authority through "clean" layouts and a lack of decorative fluff.

## Colors

The palette centers on the three core service types of the NYC TLC dataset, using color to differentiate Yellow Taxi, Green Taxi, and High Volume FHV (Apps).

- **Yellow Taxi (#F3C613):** Represents the classic Yellow Taxi service. Used for charts, badges, and highlights related to Yellow Taxi data.
- **Green Taxi (#2A9D8F):** Represents Green Taxi (Boro Taxi) service. Used for charts, badges, and highlights related to Green Taxi data.
- **High Volume FHV / Apps (#5F9EA0 / #4B0082):** Represents app-based services (Uber, Lyft, etc.). Purple-blue tones are used for charts, badges, and highlights related to FHV data.
- **Alerts / Rejections (#E63946):** Used for warnings, rejected trips, fraud alerts, and data-quality issues.
- **Success / Retained (#2A9D8F):** Used for successful operations, retained data, and positive indicators.

## Layout & Unified Application Wrapper

Toda la aplicación debe estar envuelta en un layout unificado:

### Sidebar Izquierdo (Navegación)
- Menú para saltar entre los distintos dashboards: Volumen, Financiero, Fraude, Data Quality.
- Debe resaltar la sección activa.
- Colapsable en dispositivos móviles.

### Panel de Filtros (Derecho o Superior)
- Segmentadores globales (Slicers) aplicables a todos los dashboards.
- Controles incluidos:
  - **Checkbox / Dropdown de Borough:** Manhattan, Brooklyn, Queens, Bronx, Staten Island.
  - **Checkbox de Tipo de Servicio:** Yellow Taxi, Green Taxi, High Volume FHV.
  - **Date Picker para Rango de Fechas:** Selección de rango de fechas para filtrar los datos.

### Background / Theme
- **Light Mode:** Fondo claro con tarjetas blancas (`bg-white rounded-xl shadow-sm`).
- **Dark Mode (opcional):** Fondo oscuro suave (`bg-gray-900` con tarjetas `bg-gray-800`).

## Typography

The design system employs **Public Sans**, a humanist sans-serif designed for government use. It provides exceptional legibility and an institutional tone.

- **Headlines:** Use Bold weights (700) for primary headings to establish clear hierarchy. For deep hierarchy, use the Primary Red sparingly on section headers.
- **Body Text:** Standardize on 16px (Body-MD) for maximum readability. Line height is kept generous (1.5x) to facilitate the scanning of long-form reports.
- **Labels:** Use Semi-Bold (600) for form labels and metadata, ensuring they are distinguishable from body text even at smaller sizes.

## Spacing

This design system follows a **Fixed Grid** philosophy for desktop to emulate a structured document layout.

- **Desktop:** 12-column grid with a maximum content width of 1200px. Gutters are fixed at 24px.
- **Mobile:** Fluid 4-column grid with 16px side margins.
- **Spacing Rhythm:** Based on a 4px/8px baseline. Use `stack-lg` (32px) to separate major sections, and `stack-md` (16px) for internal component spacing. Generous whitespace is essential to the "official" look—avoid crowding elements.

## Elevation & Depth

To maintain a minimalist, flat aesthetic, this design system avoids heavy drop shadows.

- **Flat Planes:** Surfaces are differentiated by color (White vs. Surface-Muted) rather than shadows.
- **Low-Contrast Outlines:** Use 1px borders (#E5E7EB) to define cards and containers.
- **Interactive States:** Only use subtle, soft shadows on hover for actionable items (buttons, cards) to indicate "lift" without appearing skeuomorphic. Depth is primarily communicated through layering and contrast.

## Shapes

The shape language is conservative and professional.

- **Soft Edges:** A universal radius of 0.25rem (4px) is applied to buttons, input fields, and cards. This provides a modern touch while maintaining a serious, structured appearance.
- **Strict Geometry:** Avoid circular "pill" shapes unless used for status indicators (chips). Containers should remain rectangular with soft corners to align with the grid-based, document-centric layout.

## Components

- **Buttons:** Primary buttons use the Primary Red (#C81912) with white text. Secondary buttons use an outline style or the Deep Blue.
- **Input Fields:** Use a light gray background fill (#F3F4F6) with a bottom-border or subtle 1px frame. Labels must be positioned above the field for maximum accessibility.
- **Cards:** Cards are white with a 1px subtle border. They should not have shadows unless hovered. Use cards to group related statistics or document links.
- **Chips/Badges:** Used for status (e.g., "Published," "Draft"). Use squared-off or slightly rounded shapes with low-saturation background colors and high-contrast text.
- **Lists:** Data-heavy lists should use "zebra striping" with Surface-Muted gray and clear dividers to ensure row legibility.
- **Breadcrumbs:** Essential for navigation in a government portal. Use Label-MD typography in Deep Blue to show the user's location within the site hierarchy.

---

(End of file - total 167 lines)
